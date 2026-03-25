use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime};

use metrics::gauge;
use smol::net::{SocketAddr, SocketAddrV4, SocketAddrV6};
use smol::{Timer, channel::*, stream::Stream};

use crate::addr_generisch::Addr;
use crate::dht_knoten::Anfrageergebnis;
use crate::{
	Fehler,
	datentypen::{KnotenInfo, U160},
	dht_knoten::{DhtKnoten, InfoHashMitKnoten},
	krpc::{KrpcAnfrage, KrpcAntwort, KrpcFehler, KrpcFehlercode},
};

/// Minimale Wartezeit bevor ein Knoten neu gescannt werden darf. Muss
/// mindestens 6 Stunden sein.
const ZEITGRENZE: Duration = Duration::from_hours(6);

#[allow(non_upper_case_globals)]
const KNOTENPUFFERGRÖßE: usize = 2048;

/// Wenn der Knotenpuffer unter diese Größe fällt versuchen wir agressiver neue
/// Knoten zu finden.
const KNOTEN_NACHSCAN_SCHWELLWERT: usize = 1536;

struct KnotenSperrliste {
	erledigt_a: HashSet<U160>,
	erledigt_b: HashSet<U160>,
	zeitstempel: Instant,
}
impl KnotenSperrliste {
	fn neu() -> Self {
		KnotenSperrliste {
			erledigt_a: HashSet::new(),
			erledigt_b: HashSet::new(),
			zeitstempel: Instant::now(),
		}
	}

	/// Fügt die gegebene Knoten-Id ein und gibt zurück ob diese bereits vorhanden
	/// war.
	fn gesehen(&mut self, id: U160) -> bool {
		if self.zeitstempel.elapsed() > ZEITGRENZE {
			std::mem::swap(&mut self.erledigt_a, &mut self.erledigt_b);
			self.erledigt_a.clear();
			self.zeitstempel = Instant::now();
		}
		let vorhanden =
			self.erledigt_a.contains(&id) || self.erledigt_b.contains(&id);

		if !vorhanden {
			self.erledigt_a.insert(id);
		}

		vorhanden
	}
}

pub struct Scanner<A: Addr + 'static> {
	knoten_rx: Receiver<(U160, SocketAddr)>,
	knoten_tx: Sender<(U160, SocketAddr)>,
	info_hash_rx: Receiver<InfoHashMitKnoten>,
	info_hash_tx: Sender<InfoHashMitKnoten>,
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
		let (knoten_tx, knoten_rx) = bounded(KNOTENPUFFERGRÖßE);
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

	pub fn scannen(self: Arc<Self>) -> impl Stream<Item = InfoHashMitKnoten> {
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
					smol::spawn(async move {
						if let Err(e) = self_arc2.knoten_scannen(k).await {
							log::warn!("Fehler beim Scannen: {e}");
						}
					}).detach();
					anfang = false;
				},
				Err(_) => {
					if letze_zufallssuche.elapsed().unwrap() > Duration::from_secs(30) &&
						!anfang
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

	/// Versucht die entsprechenden Knoten in unsere Liste einzufügen und gibt
	/// und gibt zurück ob mindestens ein Knoten dabei war den wir nicht schon
	/// gesehen haben.
	fn neue_knoten_einfügen(
		&self,
		knoten_v4: Option<Vec<KnotenInfo<SocketAddrV4>>>,
		knoten_v6: Option<Vec<KnotenInfo<SocketAddrV6>>>,
	) -> bool {
		// TODO zu kompliziert
		let knoten_res: Option<Vec<(U160, SocketAddr)>> = if A::IST_IPV4 {
			knoten_v4.map(|v| v.into_iter().map(|k| (k.id, k.addr.into())).collect())
		} else {
			knoten_v6.map(|v| v.into_iter().map(|k| (k.id, k.addr.into())).collect())
		};
		let knoten = knoten_res.unwrap_or_default();

		let mut neuer_zielknoten = false;
		let knoten_len = knoten.len();
		for k in knoten {
			neuer_zielknoten |= !self.zielpuffer_push(k);
		}
		gauge!("Scanner Zielpuffer").set(self.knoten_rx.len() as f64);

		log::trace!(
			"neue_knoten_einfügen: anz:{knoten_len}, neue:{neuer_zielknoten})"
		);
		neuer_zielknoten
	}

	
	// TODO Diese Monströsität umschreiben.
	async fn knoten_scannen(
		&self,
		knoten: (U160, SocketAddr),
	) -> Result<(), Fehler> {
		log::trace!("knoten_scannen: {}", knoten.1);
		let mut sample_infohashes = true;
		let mut zielnähe_bits = 0;
		// Die Entfernung zum Knoten der unserem Zielknoten am nächsten ist.
		let mut letzte_entfernung = U160([255; 20]);
		loop {
			// Wenn wir sowieso genug Knoten in Reserve haben geben wir einfach auf.
			if (!sample_infohashes || zielnähe_bits > 0)
				&& self.knoten_rx_extra.len() + self.knoten_rx.len()
					>= KNOTEN_NACHSCAN_SCHWELLWERT
			{
				break;
			}

			match self
				.anfrage_senden(knoten, zielnähe_bits, sample_infohashes)
				.await?
			{
				Anfrageergebnis::Fehler(f)
					if f.fehlercode == KrpcFehlercode::UnbekannteMethode
						&& sample_infohashes =>
				{
					sample_infohashes = false;
					continue;
				}
				Anfrageergebnis::Ok(aw) => {
					let (knoten_v4, knoten_v6) = match aw {
						KrpcAntwort::FindNode {
							knoten_v4,
							knoten_v6,
						} => (knoten_v4, knoten_v6),
						KrpcAntwort::SampleInfohashes {
							knoten_v4,
							knoten_v6,
							info_hashes,
							..
						} => {
							for info_hash in info_hashes {
								self
									.info_hash_tx
									.send(InfoHashMitKnoten {
										info_hash,
										knoten_id: knoten.0,
										addr: knoten.1,
									})
									.await
									.unwrap();
							}

							(knoten_v4, knoten_v6)
						}
						_ => break,
					};

					let neue_entfernung = knoten_v4
						.as_ref()
						.unwrap_or(&Vec::new())
						.iter()
						.map(|k| k.id)
						.chain(
							knoten_v6
								.as_ref()
								.unwrap_or(&Vec::new())
								.iter()
								.map(|k| k.id),
						)
						.map(|id| id ^ knoten.0)
						.min()
						.unwrap_or(U160([255; 20]));
					
					
					// Wenn wir von diesem Knoten keinen näheren bekommen geben wir auf.
					if neue_entfernung >= letzte_entfernung {
						break;
					}
					
					letzte_entfernung = neue_entfernung;
					if !self.neue_knoten_einfügen(knoten_v4, knoten_v6) {
						sample_infohashes = false;
						zielnähe_bits += 1;
					} else {
						break;
					}
				}
				m => {
					log::debug!("{m:?}");
					break;
				}
			}
		}

		Ok(())
	}

	async fn anfrage_senden(
		&self,
		knoten: (U160, SocketAddr),
		zielnähe_bits: usize,
		sample_infohashes: bool,
	) -> Result<Anfrageergebnis, Fehler> {
		let anf = if sample_infohashes {
			KrpcAnfrage::SampleInfohashes {
				ziel: knoten.0.n_bits_beibehalten(zielnähe_bits),
				will: None,
			}
		} else {
			KrpcAnfrage::FindNode {
				ziel: knoten.0.n_bits_beibehalten(zielnähe_bits),
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
				"Scanner",
				!sample_infohashes,
			)
			.await?;

		// Das unwrap ist nur für den Kanal der nie versagen sollte.
		Ok(f.await.unwrap())
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
				.knoten_iterativ_suchen(U160::zufällig(), info_tx)
				.await
		})
		.detach();
	}

	/// Fügt den gegebenen Knoten ein und gibt zurück ob dieser bereits
	/// vorhanden war. Wenn KNOTENPUFFERGRÖßE erreicht ist werden die
	/// ältesten Knoten gelöscht.
	fn zielpuffer_push(&self, obj: (U160, SocketAddr)) -> bool {
		let schon_gesehen = self.sperrliste.lock().unwrap().gesehen(obj.0);
		if !schon_gesehen {
			self.knoten_tx.force_send(obj).unwrap();
		}

		schon_gesehen
	}
}
