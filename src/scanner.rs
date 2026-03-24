use std::{collections::{HashSet, VecDeque}, rc::Rc, sync::Arc, time::{Duration, Instant}};
use std::sync::Mutex;

use smol::{channel::*, stream::Stream};
use smol::net::{SocketAddr, SocketAddrV4, SocketAddrV6};
use smol::LocalExecutor;

use crate::{Fehler, datentypen::{KnotenInfo, U160}, dht_knoten::{DhtKnoten, InfoHashMitKnoten}, krpc::{KrpcAnfrage, KrpcAntwort, KrpcFehler, KrpcFehlercode}};
use crate::dht_knoten::Anfrageergebnis;
use crate::addr_generisch::Addr;

/// Minimale Wartezeit bevor ein Knoten neu gescannt werden darf. Muss
/// mindestens 6 Stunden sein.
const ZEITGRENZE : Duration = Duration::from_hours(6);

#[allow(non_upper_case_globals)]
const KNOTENPUFFERGRÖßE : usize = 2048;

/// Wenn der Knotenpuffer unter diese Größe fällt versuchen wir agressiver neue
/// Knoten zu finden.
const KNOTEN_NACHSCAN_SCHWELLWERT : usize = 1536;

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
			zeitstempel: Instant::now()
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
		let vorhanden = self.erledigt_a.contains(&id) || 
			self.erledigt_b.contains(&id);
		
		if !vorhanden {
			self.erledigt_a.insert(id);
		}
		
		vorhanden
	}
}

pub struct Scanner<A: Addr> {
	zielpuffer: Mutex<VecDeque<(U160, SocketAddr)>>,
	info_hash_rx: Receiver<InfoHashMitKnoten>,
	info_hash_tx: Sender<InfoHashMitKnoten>,
	sperrliste: Mutex<KnotenSperrliste>,
	rx_knoten: Receiver<(U160, SocketAddr)>,
	dht_knoten: Rc<DhtKnoten<A>>,
	
}
impl<A: Addr + 'static> Scanner<A> {
	pub fn neu(
		dht_knoten: DhtKnoten<A>,
		rx_knoten: Receiver<(U160, SocketAddr)>,
	) -> Self {
		let (info_hash_tx, info_hash_rx) = unbounded();
		Scanner {
			zielpuffer: Mutex::new(VecDeque::new()),
			sperrliste: Mutex::new(KnotenSperrliste::neu()),
			info_hash_rx,
			info_hash_tx,
			dht_knoten: Rc::new(dht_knoten),
			rx_knoten,
		}
	}
	
	/*
	pub fn scannen<'ex>(self, async_exec: LocalExecutor<'ex>) -> (impl Stream<Item=InfoHashMitKnoten>, LocalExecutor<'ex>) {
		let rx = self.info_hash_rx.clone();
		
		// let rc = self.dht_knoten.clone();
		let arc_self = Arc::new(self);
		let arc2= arc_self.clone();
		
		
		// rc.starten(bootstrap_knoten, &arc_self.executor);
		
		let f = arc2.scannen_intern(&async_exec);
		
		async_exec.spawn(f).detach();
		(rx, async_exec)
	}
	*/
	
	async fn scannen_intern<'ex>(self: Arc<Self>, async_exec: &LocalExecutor<'ex> ) -> Result<(), Fehler> {
		loop {
			self.extra_knoten_einlesen();
			let arc2 = self.clone();
			
			match self.zielpuffer.lock().unwrap().pop_front() {
				Some(k) => async_exec.spawn(async move {
					if let Err(e) = arc2.knoten_scannen(k).await {
						log::warn!("Fehler beim Scannen: {e}");
					}
				}).detach(),
				None => {
					self.zufallssuche().await;
				}
			}
		}
	}
	/*
	fn antwort_verarbeiten(&mut self, aw_res: Anfrageergebnis) {
		// TODO bei unbekannter Methode find_nodes benutzen
		let aw = if let Anfrageergebnis::Ok(a) = aw_res { a } else {
			return;
		};
		
		match aw {
			KrpcAntwort::SampleInfohashes { knoten_v4, knoten_v6, info_hashes, .. } => {
				self.antwort_verarbeiten_neue_knoten(knoten_v4, knoten_v6);
				
				// TODO
			},
			KrpcAntwort::FindNode { knoten_v4, knoten_v6 } => {
				self.antwort_verarbeiten_neue_knoten(knoten_v4, knoten_v6);
			},
			_ => {}
		}
	}
	*/
	
	/// Versucht die entsprechenden Knoten in unsere Liste einzufügen und gibt
	/// und gibt zurück ob mindestens ein Knoten dabei war den wir nicht schon
	/// gesehen haben.
	fn neue_knoten_einfügen(
		&self,
		knoten_v4: Option<Vec<KnotenInfo<SocketAddrV4>>>,
		knoten_v6: Option<Vec<KnotenInfo<SocketAddrV6>>>,
	) -> bool {
		// TODO zu kompliziert
		let knoten_res: Option<Vec<(U160, SocketAddr)>> =
			if A::IST_IPV4 {
			knoten_v4.map(|v| v.into_iter().map(|k| (
				k.id,
				k.addr.into()
			)).collect())
		} else {
			knoten_v6.map(|v| v.into_iter().map(|k| ( 
				k.id,
				k.addr.into()
			)).collect())
		};
		
		let mut neuer_zielknoten = false;
		for k in knoten_res.unwrap_or_default() {
			neuer_zielknoten |= !self.zielpuffer_push(k);
		}
		
		neuer_zielknoten
	}
	
	async fn knoten_scannen(
		&self,
		knoten: (U160, SocketAddr),
	) -> Result<(), Fehler> {
		let mut sample_infohashes = true;
		let mut zielnähe_bits = 0;
		// Die Entfernung zum Knoten der unserem Zielknoten am nächsten ist.
		let mut letzte_entfernung = U160([255;20]);
		loop {
			// Wenn wir sowieso genug Knoten in Reserve haben geben wir einfach auf.
			if (!sample_infohashes || zielnähe_bits > 0) && 
				self.zielpuffer.lock().unwrap().len() >= KNOTEN_NACHSCAN_SCHWELLWERT
			{
				break;
			}
			
			match self.anfrage_senden(knoten, zielnähe_bits, sample_infohashes)
				.await?
			{
				Anfrageergebnis::Fehler(f)
					if f.fehlercode == KrpcFehlercode::UnbekannteMethode &&
					sample_infohashes => 
				{
					sample_infohashes = false;
					continue;
				},
				Anfrageergebnis::Ok(aw) => {
					let (knoten_v4, knoten_v6 ) = match aw {
						KrpcAntwort::FindNode { knoten_v4, knoten_v6 } =>
							(knoten_v4, knoten_v6),
						KrpcAntwort::SampleInfohashes {
							knoten_v4,
							knoten_v6,
							info_hashes,
							..
						} => {
							for info_hash in info_hashes {
								self.info_hash_tx.send(InfoHashMitKnoten {
									info_hash,
									knoten_id: knoten.0,
									addr: knoten.1,
								}).await.unwrap();
							}
							
							(knoten_v4, knoten_v6)
						},
						_ => break,
					};
					
					let neue_entfernung = knoten_v4.as_ref().unwrap_or(&Vec::new())
						.iter().map(|k| k.id)
						.chain(knoten_v6.as_ref().unwrap_or(&Vec::new()).iter().map(|k| k.id))
						.map(|id| id ^ knoten.0).min().unwrap_or(U160([255;20]));
					// Wenn wir von diesem Knoten keinen näheren bekommen geben wir auf.
					if neue_entfernung >= letzte_entfernung {
						break;
					}
					letzte_entfernung = neue_entfernung;
					if !self.neue_knoten_einfügen(knoten_v4, knoten_v6) {
						zielnähe_bits += 1;
					}
				}
				_ => break,
			}
		}
		
		Ok(())
	}
	
	async fn anfrage_senden(
		&self,
		knoten: (U160, SocketAddr),
		zielnähe_bits: usize,
		sample_infohashes: bool
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
		
		let f = self.dht_knoten.anfrage_senden(
			KnotenInfo {
				id: knoten.0,
				addr: A::aus_socket_addr(knoten.1).unwrap(),
				
			},
			anf,
			"Scanner",
			!sample_infohashes).await?;
			
		// Das unwrap ist nur für den Kanal der nie versagen sollte.
		Ok(f.await.unwrap())
	}
	
	/*
	async fn suchschritt(&mut self) -> Result<(), Fehler> {
		// unwrap: es wurde voher sichergestellt, dass mindestens ein
		//         Knoten  verfügbar ist.
		let anf_an = self.zielpuffer.lock().unwrap().pop_front().unwrap();
		
		let anf = KrpcAnfrage::SampleInfohashes {
			ziel: anf_an.0.invertieren(),
			will: None,
		};
		
		let f = self.dht_knoten.anfrage_senden(
			KnotenInfo {
				id: anf_an.0,
				addr: A::aus_socket_addr(anf_an.1).unwrap(),
				
			},
			anf,
			"Scanner",
			false).await?;
			
		
		let tx2 = self.info_hash_tx.clone();
		smol::spawn(async move {
			tx2.send(f.recv().unwrap()).await.unwrap()
		}).detach();
		
		Ok(())
	}
	*/
	
	async fn zufallssuche(&self) {
		let neue_knoten = self
			.dht_knoten
			.knoten_iterativ_suchen(U160::zufällig())
			.await;
		
		if neue_knoten.is_empty() { return; }
		self.zielpuffer
			.lock()
			.unwrap()
			.extend(neue_knoten.into_iter().map(|k| (k.id, k.addr.into())));
	}
	
	fn extra_knoten_einlesen(&self) {
		while let Ok(x) = self.rx_knoten.try_recv() {
			self.zielpuffer_push(x);
		}
	}
	
	/// Fügt den gegebenen Knoten ein und gibt zurück ob dieser bereits 
	/// vorhanden war. Wenn KNOTENPUFFERGRÖßE erreicht ist werden die
	/// ältesten Knoten gelöscht.
	fn zielpuffer_push(&self, obj: (U160, SocketAddr)) -> bool {
		let mut zielpuffer = self.zielpuffer.lock().unwrap();
		let schon_gesehen = self.sperrliste.lock().unwrap().gesehen(obj.0);
		if schon_gesehen {
			while zielpuffer.len() >= KNOTENPUFFERGRÖßE - 1 {
				zielpuffer.pop_front();
			}
			zielpuffer.push_back((obj.0, obj.1));
		}
		
		schon_gesehen
	}
}
