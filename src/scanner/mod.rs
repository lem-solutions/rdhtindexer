use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use metrics::*;
use smol::net::{SocketAddr, SocketAddrV4, SocketAddrV6};
use smol::{Timer, channel::*, stream::Stream};

use crate::addr_generisch::Addr;
use crate::dht_knoten::Anfrageergebnis;
use crate::dht_knoten::InfoHashesMitKnoten;
use crate::tempomat::Prio;
use crate::{
	Fehler,
	datentypen::{KnotenInfo, U160},
	dht_knoten::DhtKnoten,
	krpc::{KrpcAnfrage, KrpcAntwort},
};

mod sperrliste;
use sperrliste::*;

#[allow(non_upper_case_globals)]
const KNOTENPUFFERGRÖßE: usize = 10000;

pub struct Scanner<A: Addr + 'static> {
	knoten_rx: Receiver<(U160, SocketAddr)>,
	knoten_tx: Sender<(U160, SocketAddr)>,
	info_hash_rx: Receiver<InfoHashesMitKnoten>,
	info_hash_tx: Sender<InfoHashesMitKnoten>,
	sperrliste: Mutex<KnotenSperrliste>,
	knoten_rx_extra: Receiver<(U160, SocketAddr)>,
	dht_knoten: Arc<DhtKnoten<A>>,
}
impl<A: Addr> Scanner<A> {
	pub fn neu(
		dht_knoten: Arc<DhtKnoten<A>>,
		rx_knoten: Receiver<(U160, SocketAddr)>,
	) -> Self {
		let (info_hash_tx, info_hash_rx) = unbounded();
		let (knoten_tx, knoten_rx) = unbounded();
		Scanner {
			knoten_tx,
			knoten_rx,
			sperrliste: Mutex::new(KnotenSperrliste::neu()),
			info_hash_rx,
			info_hash_tx,
			dht_knoten: dht_knoten,
			knoten_rx_extra: rx_knoten,
		}
	}

	pub fn scannen(self: Arc<Self>) -> impl Stream<Item = InfoHashesMitKnoten> {
		let rx = self.info_hash_rx.clone();
		smol::spawn(self.scannen_intern()).detach();
		rx
	}

	async fn scannen_intern(self: Arc<Self>) -> Result<(), Fehler> {
		let mut letze_zufallssuche = std::time::SystemTime::UNIX_EPOCH;
		let mut anfang = true;
		loop {
			let self_arc2 = self.clone();

			while let Ok(k) = self.knoten_rx_extra.try_recv() {
				self.zielpuffer_push(k);
			}

			gauge!("Scanner Zielpuffer").set(self.knoten_rx.len() as f64);

			match self.knoten_rx.try_recv() {
				Ok(k) => {
					if let Err(e) = self_arc2.knoten_scannen(k).await {
						log::warn!("Fehler beim Scannen: {e}");
					}
					anfang = false;
				}
				Err(_) => {
					if letze_zufallssuche.elapsed().unwrap() > Duration::from_secs(30)
						&& !anfang
					{
						log::debug!("Scanner: neue Zufallssuche");
						let self2 = self.clone();
						smol::spawn(async move { self2.zufallssuche().await }).detach();
						letze_zufallssuche = SystemTime::now();
					} else {
						Timer::after(Duration::from_secs(1)).await;
					}
				}
			}
		}
	}

	async fn knoten_scannen(
		self: Arc<Self>,
		knoten: (U160, SocketAddr),
	) -> Result<(), Fehler> {
		// Wir warten bis die Anfrage abgeschickt ist(sodass wir
		// blockiert werden wenn die maximale Anzahl an ausstehenden
		// Anfragen in `DhtKnoten` erreicht ist)
		let self2 = self.clone();
		let aw_fut = self.anfrage_senden(knoten, true).await?;

		// Aber wir warten nicht auf die Antwort.
		let fut = async move {
			let aw = aw_fut.await.unwrap();
			self2.antwort_verarbeiten(aw, knoten).await;
		};

		smol::spawn(fut).detach();

		Ok(())
	}

	async fn antwort_verarbeiten(
		self: Arc<Self>,
		erg: Anfrageergebnis,
		knoten: (U160, SocketAddr),
	) {
		match erg {
			Anfrageergebnis::Ok(KrpcAntwort::FindNode {
				knoten_v4,
				knoten_v6,
			}) => self.neue_knoten_einfügen(knoten_v4, knoten_v6),

			Anfrageergebnis::Ok(KrpcAntwort::SampleInfohashes {
				knoten_v4,
				knoten_v6,
				info_hashes,
				interval_sek,
				anz_infohashes,
			}) => {
				self
					.antwort_verarbeiten_sample_infohashes(
						knoten_v4,
						knoten_v6,
						info_hashes,
						interval_sek,
						anz_infohashes,
						knoten,
					)
					.await
			}

			Anfrageergebnis::Ok(_) => unreachable!(
				"Die korrekte Methode sollte durch `Anfragenpuffer` garantiert sein."
			),

			erg => log::debug!("AW {erg:?}"),
		}
	}

	async fn antwort_verarbeiten_sample_infohashes(
		self: Arc<Self>,
		knoten_v4: Option<Vec<KnotenInfo<SocketAddrV4>>>,
		knoten_v6: Option<Vec<KnotenInfo<SocketAddrV6>>>,
		info_hashes: Vec<U160>,
		interval_sek: Option<u16>,
		anz_infohashes: Option<usize>,
		knoten: (U160, SocketAddr),
	) {
		if let Some(x) = anz_infohashes {
			histogram!("sample_infohashes anz_infohashes").record(x as f64);
		} else {
			counter!("smaple_infohashes anz_infohashes None").increment(1);
		}
		if let Some(x) = interval_sek {
			histogram!("sample_infohashes interval_sek").record(x as f64);
		} else {
			counter!("smaple_infohashes interval_sek None").increment(1);
		}

		self
			.info_hash_tx
			.send(InfoHashesMitKnoten {
				info_hashes,
				knoten_id: knoten.0,
				addr: knoten.1,
			})
			.await
			.unwrap();

		self.neue_knoten_einfügen(knoten_v4, knoten_v6);
	}

	/// Versucht die entsprechenden Knoten in unsere Liste einzufügen.
	fn neue_knoten_einfügen(
		&self,
		knoten_v4: Option<Vec<KnotenInfo<SocketAddrV4>>>,
		knoten_v6: Option<Vec<KnotenInfo<SocketAddrV6>>>,
	) {
		// TODO zu kompliziert
		let knoten_res: Option<Vec<(U160, SocketAddr)>> = if A::IST_IPV4 {
			knoten_v4.map(|v| v.into_iter().map(|k| (k.id, k.addr.into())).collect())
		} else {
			knoten_v6.map(|v| v.into_iter().map(|k| (k.id, k.addr.into())).collect())
		};
		let knoten = knoten_res.unwrap_or_default();

		let knoten_len = knoten.len();
		for k in knoten {
			self.zielpuffer_push(k);
		}
		gauge!("Scanner Zielpuffer").set(self.knoten_rx.len() as f64);

		log::trace!("neue_knoten_einfügen: anz:{knoten_len}");
	}

	async fn anfrage_senden(
		self: Arc<Self>,
		knoten: (U160, SocketAddr),
		sample_infohashes: bool,
	) -> Result<
		impl Future<Output = Result<Anfrageergebnis, oneshot::RecvError>>,
		Fehler,
	> {
		let anf = if sample_infohashes {
			KrpcAnfrage::SampleInfohashes {
				ziel: U160::zufällig(),
				will: None,
			}
		} else {
			KrpcAnfrage::FindNode {
				ziel: U160::zufällig(),
				will: None,
			}
		};

		let f = self
			.dht_knoten
			.anfrage_senden(
				KnotenInfo {
					id: knoten.0,
					addr: A::aus_socket_addr(knoten.1).unwrap(),
				},
				anf,
				Prio::Scannen,
				!sample_infohashes,
			)
			.await?;

		// Das unwrap ist nur für den Kanal der nie versagen sollte.
		Ok(f)
	}

	async fn zufallssuche(self: Arc<Self>) {
		let (info_tx, info_rx): (Sender<KnotenInfo<A>>, _) =
			smol::channel::unbounded();
		let self2 = self.clone();
		smol::spawn(async move {
			while let Ok(k) = info_rx.recv().await {
				self2.zielpuffer_push((k.id, k.addr.into()));
				gauge!("Scanner Zielpuffer").set(self2.knoten_rx.len() as f64);
			}
		})
		.detach();

		let self3 = self.clone();
		smol::spawn(async move {
			self3
				.dht_knoten
				.knoten_iterativ_suchen(U160::zufällig(), info_tx, Prio::Scannen)
				.await
		})
		.detach();
	}

	/// Fügt den gegebenen Knoten ein und gibt zurück ob dieser bereits
	/// vorhanden war. Wenn KNOTENPUFFERGRÖßE erreicht ist werden die
	/// ältesten Knoten gelöscht.
	fn zielpuffer_push(&self, obj: (U160, SocketAddr)) {
		if self.knoten_tx.len() >= KNOTENPUFFERGRÖßE {
			return;
		}

		if !self.sperrliste.lock().unwrap().gesehen(obj.0) {
			self.knoten_tx.try_send(obj).unwrap();
		}
	}
}
