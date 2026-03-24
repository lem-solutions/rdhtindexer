use std::sync::Arc;
use bendy::encoding::ToBencode;
use oneshot::channel;
use smol::net::{UdpSocket, IpAddr, SocketAddr};
use smol::io::Error as IoError;
use std::cell::RefCell;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};
use smol::channel::*;
use std::collections::HashSet;
use crate::datentypen::{KnotenInfo, U160};
use crate::addr_generisch::Addr;

use crate::tempomat::*;
use crate::krpc::*;
use crate::Fehler;

mod routing_tabelle;
mod peer_tabelle;
mod anfragenpuffer;
mod token;
use routing_tabelle::*;
use anfragenpuffer::*;
pub use anfragenpuffer::Anfrageergebnis;
use peer_tabelle::PeerTabelle;
use token::{token_generieren, token_überprüfen};


const VERSIONSCODE : Option<&'static [u8]> = None;

// https://bittorrent.org/beps/bep_0032.html
const MAX_KRPC_LEN : usize = 1024;

const ANZ_MELDUNGEN_IPWECHSEL : usize = 10;
const BEP51_INTERVAL_SEK : u16 = 300;

pub struct InfoHashMitKnoten {
	pub info_hash: U160,
	pub knoten_id: U160,
	pub addr: SocketAddr,
}

#[derive(Clone)]
pub struct KnotenKanäle {
	pub knoten: Sender<(U160, SocketAddr)>,
	pub info_hash_mit_peer: Sender<(U160, SocketAddr)>,
	pub info_hash_mit_knoten: Sender<InfoHashMitKnoten>,
}
impl KnotenKanäle {
	pub fn neu(puffergröße: usize) -> (KnotenKanäle, KnotenEmpfänger) {
		let (knoten_tx, knoten_rx) = bounded(puffergröße);
		let (info_hash_mit_peer_tx, info_hash_mit_peer_rx) = bounded(puffergröße);
		let (info_hash_mit_knoten_tx, info_hash_mit_knoten_rx) = bounded(puffergröße);
		
		(
			KnotenKanäle {
				knoten: knoten_tx,
				info_hash_mit_peer: info_hash_mit_peer_tx,
				info_hash_mit_knoten: info_hash_mit_knoten_tx,
			},
			KnotenEmpfänger {
				knoten: knoten_rx,
				info_hash_mit_peer: info_hash_mit_peer_rx,
				info_hash_mit_knoten: info_hash_mit_knoten_rx,
			}
		)
	}
}

pub struct KnotenEmpfänger {
	pub knoten: Receiver<(U160, SocketAddr)>,
	pub info_hash_mit_peer: Receiver<(U160, SocketAddr)>,
	pub info_hash_mit_knoten: Receiver<InfoHashMitKnoten>,
}

pub struct DhtKnoten<A: Addr> {
	routing_tabelle: RefCell<RoutingTabelle<A>>,
	peer_tabelle: RefCell<PeerTabelle<A>>,
	ausstehende_anfragen: RefCell<Anfragenpuffer>,
	tempomat: Arc<Tempomat>,
	udp: UdpSocket,
	kanäle: KnotenKanäle,
	
	externe_addresse: RefCell<Option<A>>,
	angebliche_externe_addresse: RefCell<Option<A>>,
	quellen_für_externe_addresse: RefCell<HashSet<A>>,
	
	max_ausstehende_anfragen: usize,
	anfragen_zeitgrenze: Duration,
	gestartet: AtomicBool,
}

impl<A: Addr> DhtKnoten<A> {
	pub async fn neu(
		addr: A,
		tempomat: Arc<Tempomat>,
		kanäle: KnotenKanäle,
		max_ausstehende_anfragen: usize,
		anfragen_zeitgrenze: Duration,
		max_peer_tabellen_größe: usize,
		max_peer_tabellen_alter: Duration,
	) -> Result<Self, IoError> {
		Ok(DhtKnoten {
			// Die IP Addresse ist wahrscheinlich nicht die externe, deswegen
			// wird die routing Tabelle wahrscheinlich schnell neu erstellt
			routing_tabelle: RefCell::new(RoutingTabelle::neu(U160::bep42_generieren(&addr.ip()))),
			// max_peers_pro_torrent: wir brauchen nur so viele wie in ein Paket passen.
			//                        16 ist definitiv genug und sollte problemlos passen.
			 
			// Bei bep51_auswahl_größe gilt je mehr desdo besser solange es in ein Paket passt.
			// TODO: Maximal möglichen Wert ausrechnen und benutzen, beachten dass
			//       beim beantworten einer Anfrage die maximale Antwortgröße geringer
			//       sein kann. z. B. wegen einer längeren Transaktionsnummer.
			peer_tabelle: RefCell::new(PeerTabelle::neu(max_peer_tabellen_größe, 16, max_peer_tabellen_alter, 32, Duration::from_mins(30))),
			udp: UdpSocket::bind(addr).await?,
			tempomat,
			kanäle,
			ausstehende_anfragen: RefCell::new(Anfragenpuffer::neu(max_ausstehende_anfragen)),
			max_ausstehende_anfragen,
			anfragen_zeitgrenze,
			angebliche_externe_addresse: RefCell::new(None),
			quellen_für_externe_addresse: RefCell::new(HashSet::new()),
			externe_addresse: RefCell::new(None),
			gestartet: false.into(),
		})
	}
	
	pub fn starten(self, bootstrap_knoten: Vec<A>)
	where A: 'static
	{
		let bootstrap_knoten_iter = bootstrap_knoten.into_iter();
		std::thread::spawn(move || {
			let war_gestartet = self.gestartet.swap(true, std::sync::atomic::Ordering::Relaxed);
			assert!(!war_gestartet);
			
			let async_exec = smol::LocalExecutor::new();
			
			
			async_exec.spawn(self.nachrichten_empfangen()).detach();
			async_exec.spawn(self.routing_tabelle_warten()).detach();
			
			for knoten_addr in bootstrap_knoten_iter {
				async_exec.spawn(self.anfrage_senden(
					KnotenInfo { id: self.routing_tabelle.borrow().eigene_id, addr:  knoten_addr.clone()},
					KrpcAnfrage::Ping,
					"DhtKnoten::starten",
					true
				)).detach();
			}
			
			loop {
				smol::future::block_on(async_exec.tick());
			}
		});
		
		
	}
	
	async fn routing_tabelle_warten(&self) -> Fehler {
		loop {
			let rt_b = self.routing_tabelle.borrow();
			if let Some(k_info) = rt_b.fragwürdigen_knoten_finden() {
				let ziel_id = k_info.id.clone();
				let ziel_addr = k_info.addr.clone();
				std::mem::drop(rt_b);
				let req_res = self.anfrage_senden(
					KnotenInfo { id: ziel_id, addr:  ziel_addr},
					KrpcAnfrage::Ping,
					"DhtKnoten::routing_tabelle_warten",
					true
				).await;
				match req_res {
					Ok(rx) => if let Err(f) = rx.await {
						return f.into();
					},
					Err(f) => return f
				}
			} else {
				std::mem::drop(rt_b);
				// wenn es gerade keine fragwürdigen Knoten gibt warten wir einfach einwenig.
				smol::Timer::after(ROUTING_TABELLE_ZEITFENSTER / 2).await;
			}
		}
	}
	
	async fn nachrichten_empfangen(
		&self,
	) -> Result<(), Fehler> {
		const MAX_UDP_LEN : usize = 2048;
		let mut puffer = [0u8;MAX_UDP_LEN+1];
		loop {
			self.tempomat.warten_runter("DhtKnoten::nachricht_empfangen").await;
			let (udp_len, quell_addr_enum) = self.udp.recv_from(&mut puffer[..]).await?;
			let quell_addr = A::aus_socket_addr(quell_addr_enum).unwrap(); // TODO Wirklich unmöglich?
			if udp_len > MAX_UDP_LEN {
				log::warn!("UDP Paket zu groß ({udp_len} > {MAX_UDP_LEN})");
				self.tempomat.melden_runter("fehlerhafte UDP Pakete", udp_len);
				continue;
			}
			let nachricht_bytes = &puffer[..udp_len];
			
			let nachricht : KrpcNachricht<A> = match KrpcNachricht::einlesen(nachricht_bytes, |txid_bytes| {
				let txid = u16::from_be_bytes(txid_bytes.try_into().ok()?);
				self.ausstehende_anfragen.borrow().methode_für_txid(txid as usize)
			}) {
				Ok(n) => n,
				Err(e) => {
					log::debug!("Konnte UDP Paket von {quell_addr} nicht deserialisieren: {e} Inhalt: {nachricht_bytes:02X?}");
					self.tempomat.melden_runter("fehlerhafte UDP Pakete", udp_len);
					continue;
				}
			};
			
			if matches!(nachricht.inhalt, KrpcInhalt::Anfrage{..}) {
				log::trace!("RX REQ {quell_addr}");
				self.anfrage_verarbeiten(nachricht, udp_len, quell_addr).await;
			} else {
				let anfrage_info = if let Some(i) = self.ausstehende_anfragen.borrow_mut().nehmen_bytes(&nachricht.transaktionsnummer) {
					self.tempomat.melden_runter(i.aufgabenbereich, udp_len);
					i
				} else {
					log::debug!("Antwort mit ungültiger Transaktionsnummer: {:02X?} von {quell_addr}", nachricht.transaktionsnummer);
					self.tempomat.melden_runter("fehlerhafte UDP Pakete", udp_len);
					continue;
				};
				
				let erg = match nachricht.inhalt {
					KrpcInhalt::Antwort { id, ext_ip, aw } => {
						let methode = aw.methode();
						log::trace!("RX AW  {methode} {quell_addr} ");
						self.routing_tabelle.borrow_mut().antwort_erhalten(id, quell_addr);
						self.externe_addr_prüfen(ext_ip);
						
						Anfrageergebnis::Ok(aw)
					},
					KrpcInhalt::Fehler(f) => {
						let txt = &f.fehlermeldung;
						log::trace!("RX ERR {quell_addr} {txt}");
						if anfrage_info.bei_fehler_knoten_entfernen { self.routing_tabelle.borrow_mut().fehlschlag(anfrage_info.knoten_id); }
						Anfrageergebnis::Fehler(f)
					},
					KrpcInhalt::Anfrage {..} => unreachable!(),
				};
				// Ein Fehler beim Senden bedeutet das der Empfänger
				// nicht mehr existiert. In unserem Fall ist das nicht unbedingt ein
				// Fehler, falls wir uns nicht für die Antwort auf eine Anfrage
				// interessieren.
				#[allow(unused_must_use)]
				anfrage_info.sender.send(erg);
			}
		}
	}
	
	async fn anfrage_verarbeiten(
		&self,
		nachricht: KrpcNachricht<A>,
		udp_len: usize,
		quell_addr: A
	) {
		let (req_id, anf) = match nachricht.inhalt {
			KrpcInhalt::Anfrage { id, anf } => (id, anf),
			_ => unreachable!("`anfrage_verarbeiten` darf nur mit einer Anfrage als `nachricht` aufgerufen werden."),
		};
		self.tempomat.melden_runter("DhtKnoten::anfrage_verarbeiten", udp_len);
		
		if !quell_addr.global_valide() {
			log::debug!("Paket von ungültiger Addresse: {quell_addr}");
			return;
		}
		
		if self.kanäle.knoten.try_send((req_id, quell_addr.clone().into()))
			.is_err()
		{
			log::warn!("kanäle.knoten voll!");
		}
		
		let aw = match anf {
			KrpcAnfrage::Ping => Ok(KrpcAntwort::Ping),
			KrpcAnfrage::FindNode { ziel, will } => 
				self.anfrage_bearbeiten_find_node(ziel, A::will_opt(will)),
			KrpcAnfrage::GetPeers { info_hash, will } =>
				self.anfrage_bearbeiten_get_peers(info_hash, A::will_opt(will), &quell_addr, req_id),
			KrpcAnfrage::AnnouncePeer { implizieter_port, info_hash, port, token } =>
				self.anfrage_bearbeiten_announce_peer(implizieter_port, info_hash, port, token, &quell_addr, req_id),
			KrpcAnfrage::SampleInfohashes { ziel, will } =>
				self.anfrage_bearbeiten_sample_infohashes(ziel, A::will_opt(will)),
			KrpcAnfrage::UnbkannteMethode { name } => {
				let name_str = String::from_utf8_lossy(name.as_slice());
				log::debug!("Unbekannte Methode: {name_str}");
				Err(KrpcFehler {
					fehlercode: KrpcFehlercode::UnbekannteMethode,
					fehlermeldung: "unbekannte Mehtode".to_owned(),
				})
			}
		};
		
		let aw_senden_res = match aw {
			Ok(msg) => {
				self
					.routing_tabelle
					.borrow_mut()
					.anfrage_erhalten(req_id, quell_addr.clone());
				self.antwort_senden(&quell_addr, &nachricht.transaktionsnummer, msg, "Antworten auf eingehende Anfragen").await
			},
			Err(e) => {
				self.fehler_senden(quell_addr, &nachricht.transaktionsnummer, e, "Antworten auf eingehende Anfragen").await
			}
		};
		
		if let Err(e) = aw_senden_res {
			log::warn!("Fehler beim Abschicken einer Antwort auf eine Anfrage: {e}");
		}
		
		/*
		if aw_nachricht.is_ok() {
			self.routing_tabelle.borrow_mut().anfrage_erhalten(nachricht.anfrage_argumente.as_ref().unwrap().id, quell_addr);
		}
		*/
	}
	
	fn anfrage_bearbeiten_sample_infohashes(
		&self,
		ziel: U160,
		will: Will,
	) -> Result<KrpcAntwort, KrpcFehler> {
		let (knoten_v4, knoten_v6) = match self.nächste_knoten(ziel, will)? {
			KrpcAntwort::FindNode { knoten_v4: v4, knoten_v6: v6 } => (v4, v6),
			_ => unreachable!(),
		};
		
		let mut peer_tabelle_ref = self.peer_tabelle.borrow_mut();
		// Das die Hashliste nicht zu groß sein kann liegt in der Verantwortung der `PeerTabelle`.
		let hashes = peer_tabelle_ref.bep51_anfrage().clone();
		let ges_anz = peer_tabelle_ref.anz_torrents();
		std::mem::drop(peer_tabelle_ref);
		
		Ok(KrpcAntwort::SampleInfohashes {
			interval_sek: BEP51_INTERVAL_SEK, 
			knoten_v4,
			knoten_v6,
			info_hashes: hashes.clone(),
			anz_infohashes: ges_anz,
		})
	}
	
	fn anfrage_bearbeiten_announce_peer(&self, implizierter_port: bool, info_hash: U160, port: u16, token: Vec<u8>, quell_addr: &A, req_id: U160) -> Result<KrpcAntwort, KrpcFehler> {
		/*
		let args = nachricht.anfrage_argumente.as_ref().unwrap();
		match args.token {
			Some(t) if token_überprüfen(args.id, quell_addr, t) => {}
			Some(_) => return Err(KrpcFehler { fehlercode: KRPC_PROTOKOLL_FEHLER, fehlermeldung: b"fehlerhaftes Token".to_vec() }),
			None => return Err(KrpcFehler { fehlercode: KRPC_PROTOKOLL_FEHLER, fehlermeldung: b"Token erforderlich".to_vec() }),
		}
		
		let implizierter_port = args.impliziter_port.unwrap_or(false);
		*/
		
		/*
		let peer_addr = if implizierter_port {
			quell_addr.clone()
		} else if let Some(p) = port {
			let mut a = quell_addr.clone();
			a.port_ändern(p);
			a
		} else {
			return Err(KrpcFehler { fehlercode: KrpcFehlercode::ProtokollFehler, fehlermeldung: "Port fehlt".to_owned() });
		};
		*/
		
		if !token_überprüfen(req_id, quell_addr, token.as_slice()) {
			return Err(KrpcFehler { fehlercode: KrpcFehlercode::ProtokollFehler, fehlermeldung: "fehlerhaftes Token".to_owned() });
		}
		
		let peer_addr = if implizierter_port {
			quell_addr.clone()
		} else {
			let mut a = quell_addr.clone();
			a.port_ändern(port);
			a
		};
		
		if self.kanäle.info_hash_mit_peer.try_send(
			(info_hash, peer_addr.clone().into())).is_err()
		{
			log::warn!("kanäle.kanäle.info_hash_mit_peer voll!");
		}
		
		self.peer_tabelle.borrow_mut().peer_einfügen(info_hash, peer_addr);
		
		Ok(KrpcAntwort::AnnouncePeer)
	}
	
	fn anfrage_bearbeiten_get_peers<'a>(&self, info_hash: U160, will: Will, quell_addr: &A, req_id: U160) -> Result<KrpcAntwort, KrpcFehler> {
		if self.kanäle.info_hash_mit_knoten.try_send(
			InfoHashMitKnoten { info_hash, knoten_id: req_id, addr: quell_addr.clone().into()}).is_err()
		{
			log::warn!("kanäle.info_hash_mit_knoten voll!");
		}
		
		let mut peer_tabelle = self.peer_tabelle.borrow_mut();
		let peer_iter = peer_tabelle.peers_für_hash(&info_hash);
		let token = token_generieren(req_id, quell_addr).to_vec();
		if peer_iter.len() == 0 {
			let (knoten_v4, knoten_v6) = match self.nächste_knoten(info_hash, will)? {
				KrpcAntwort::FindNode { knoten_v4, knoten_v6 } =>
					(knoten_v4, knoten_v6),
				_ => unreachable!(),
			};
			Ok(KrpcAntwort::GetPeers {
				peers: None,
				knoten_v4,
				knoten_v6,
				token,
			})
		} else {
			Ok(KrpcAntwort::GetPeers { peers: Some(peer_iter.map(|a| a.clone().into()).collect()), knoten_v4: None, knoten_v6: None, token})
		}
	}
	
	fn nächste_knoten(&self, ziel: U160, will: Will) -> Result<KrpcAntwort, KrpcFehler> {
		let rt_b = self.routing_tabelle.borrow();
		let mut puffer = noalloc_vec_rs::vec::Vec::new();
		rt_b.nächste_k_knoten(ziel, &mut puffer);
		
		let knoten_v4 = if A::IST_IPV4 && will.v4() {
			Some(puffer.iter().map(|(knoten_id, addr)| KnotenInfo { id: *knoten_id, addr: addr.als_ipv4().unwrap().clone()}).collect())
		} else if will.v4() {
			Some(Vec::new())
		} else {
			None
		};
		
		let knoten_v6 = if !A::IST_IPV4 && will.v6() {
			Some(puffer.iter().map(|(knoten_id, addr)| KnotenInfo { id: *knoten_id, addr: addr.als_ipv6().unwrap().clone()}).collect())
		} else if will.v6() {
			Some(Vec::new())
		} else {
			None
		};
		
		Ok(KrpcAntwort::FindNode { knoten_v4, knoten_v6})
	}
	
	fn anfrage_bearbeiten_find_node<'a>(&self, ziel: U160, will: Will) -> Result<KrpcAntwort, KrpcFehler> {
		self.nächste_knoten(ziel, will)
	}
	
	fn externe_addr_prüfen(
		&self,
		addr_opt: Option<A>,
	) {
		if addr_opt.is_none() { return; }
		let addr = addr_opt.unwrap();
		
		if !addr.global_valide() {
			log::debug!("Invalide externe Addresse erhalten: {addr}");
			return;
		}
		
		let ext_addr = self.externe_addresse.borrow();
		if ext_addr.is_none() {
			log::info!("Externe Addresse:  {addr}");
		} else {
			log::info!("Neue externe Addresse: {} → {addr}", ext_addr.as_ref().unwrap());
		}
		std::mem::drop(ext_addr);
		
		self.externe_addr_ändern(addr);
	}
	
	fn externe_addr_ändern(
		&self,
		neue_addr: A,
	) {
		let mut routing_tabelle = self.routing_tabelle.borrow_mut();
		
		let mut alte_tabelle = RoutingTabelle::neu(U160::bep42_generieren(&neue_addr.ip()));
		std::mem::swap(&mut *routing_tabelle, &mut alte_tabelle);
		
		*self.externe_addresse.borrow_mut() = Some(neue_addr);
		*self.angebliche_externe_addresse.borrow_mut() = None;
		self.quellen_für_externe_addresse.borrow_mut().clear();
		
		for knoten_info in alte_tabelle.knoten_extrahieren() {
			routing_tabelle.knoten_info_einfügen(knoten_info);
		}
	}
	
	async fn nachricht_abschicken<'a>(
		&self,
		nachricht: &KrpcNachricht<A>,
		ziel: &A,
		aufgabenbereich: &'static str,
	) -> Result<usize, Fehler> {
		let datagramm = nachricht.to_bencode()?;
		if datagramm.len() > MAX_KRPC_LEN {
			return Err(Fehler::GesendeteNachrichtZuLang(datagramm.len()));
		}
		
		self.tempomat.warten_hoch(aufgabenbereich, datagramm.len()).await;
		
		let anz_geschrieben = self.udp.send_to(&datagramm[..], ziel).await?;
		if anz_geschrieben != datagramm.len() {
			log::warn!("UDP Datagramm Längenfehler (Puffer: {}, gesendet: {})", datagramm.len(), anz_geschrieben);
		}
		
		Ok(anz_geschrieben)
	}
	
	
	// Antwort{id: U160, ext_ip: Option<A>, aw: KrpcAntwort},
	async fn antwort_senden<'a>(
		&self,
		ziel: &A,
		tx_nummer: &'a [u8],
		aw: KrpcAntwort,
		aufgabenbereich: &'static str,
	) -> Result<(), Fehler> {
		let m = &aw.methode();
		log::trace!("TX AW  {m} {ziel}");
		let n = KrpcNachricht {
			transaktionsnummer: tx_nummer.to_vec(),
			versionscode: VERSIONSCODE.map(|v| v.to_vec()),
			inhalt: KrpcInhalt::Antwort { id: self.routing_tabelle.borrow().eigene_id, ext_ip: Some(ziel.clone()), aw}
		};
		
		self.nachricht_abschicken(
			&n,
			ziel,
			aufgabenbereich
		).await?;
		Ok(())
	}
	
	async fn fehler_senden<'a>(
		&self,
		ziel: A,
		tx_nummer: &'a [u8],
		krpc_fehler: KrpcFehler,
		aufgabenbereich: &'static str,
	) -> Result<(), Fehler> {
		let txt = &krpc_fehler.fehlermeldung;
		log::trace!("TX ERR {ziel} {txt}");
		let n = KrpcNachricht {
			transaktionsnummer: tx_nummer.to_vec(),
			versionscode: VERSIONSCODE.map(|v| v.to_vec()),
			inhalt: KrpcInhalt::Fehler(krpc_fehler)
		};
		
		self.nachricht_abschicken(
			&n,
			&ziel,
			aufgabenbereich
		).await?;
		Ok(())
	}
	
	pub async fn anfrage_senden<'a>(
		&self,
		ziel: KnotenInfo<A>,
		anf: KrpcAnfrage,
		aufgabenbereich: &'static str,
		bei_fehler_knoten_entfernen: bool,
	) -> Result<oneshot::Receiver<Anfrageergebnis>, Fehler> {
		let anf_methode = anf.methode();
		
		let mut austehende_anfragen = self.ausstehende_anfragen.borrow_mut();
		
		let (aw_sender, aw_empf) = oneshot::channel();
		let mut anfrage = AusstehendeAnfrage {
			methode: anf.methode(),
			knoten_id: ziel.id,
			zeitgrenze: Instant::now() + self.anfragen_zeitgrenze,
			aufgabenbereich,
			bei_fehler_knoten_entfernen,
			sender: aw_sender,
		};
		
		let tx_nummer = loop {
			anfrage.zeitgrenze = Instant::now() + self.anfragen_zeitgrenze;
			match austehende_anfragen.einfügen(anfrage) {
				Ok(txn) => break txn,
				Err((anf, warter)) => {
					anfrage = anf;
					std::mem::drop(austehende_anfragen);
					warter.await;
					austehende_anfragen = self.ausstehende_anfragen.borrow_mut();
				}
			}
		};
		std::mem::drop(austehende_anfragen);
		
		let tx_nummer_u16 : u16 = tx_nummer.try_into().unwrap();
		
		let n = KrpcNachricht {
			transaktionsnummer: tx_nummer_u16.to_be_bytes().as_slice().to_vec(),
			versionscode: VERSIONSCODE.map(|v| v.to_vec()),
			inhalt: KrpcInhalt::Anfrage { id: self.routing_tabelle.borrow().eigene_id, anf}
		};
		
		
		let ziel_addr = &ziel.addr;
		log::trace!("TX REQ {anf_methode} {ziel_addr}");
		self.nachricht_abschicken(&n, &ziel.addr, aufgabenbereich).await?;
		Ok(aw_empf)
	}
	
	
	// TODO: Den *korrekten* Algorithmus implementieren.
	// TODO: Anfällig für DoS durch überfüllung mit Fake-Knoten die
	//       immer näher am Ziel sind.
	/// Versucht einen Knoten mit ID `ziel` zu finden, bzw. Knoten
	/// möglichst nah zu dieser ID zu finden. Gibt alle genfundenen
	/// Knoten in sortierten Liste zurück(am nahegelegenster Knoten 
	/// zuerst)
	pub async fn knoten_iterativ_suchen(
		&self,
		ziel: U160,
	) -> Vec<KnotenInfo<A>> {
		let rt_b = self.routing_tabelle.borrow();
		let mut knoten_minivec = noalloc_vec_rs::vec::Vec::new();
		rt_b.nächste_k_knoten(ziel, &mut knoten_minivec);
		let mut knoten = Vec::with_capacity(knoten_minivec.len());
		for (id, addr) in knoten_minivec {
			knoten.push(KnotenInfo {
				id, addr: addr.clone()
			});
		}
		knoten.sort_by_key(|k| k.id ^ ziel);
		
		let mut beste_entfernung = U160([255;20]);
		
		while beste_entfernung > knoten[0].id ^ ziel {
			if knoten.is_empty() { return knoten }
			beste_entfernung = knoten[0].id ^ ziel;
			let anf = KrpcAnfrage::FindNode { ziel, will: None };
			let r1 = self.anfrage_senden(
				knoten[0].clone(),
				anf,
				"knoten_iterativ_suchen",
				true).await;
			
			let r2 = match r1 {
				Ok(a) => a,
				Err(err) => {
					log::warn!("Fehler bei knoten_iterativ_suchen: {err}");
					continue;
				}
			};
			
			// unwrap: Kann nur Err sein wenn der Sender weg ist.
			//         Das sollte nie passieren.
			let aw = match r2.await.unwrap() {
				Anfrageergebnis::Ok(aw) => aw,
				_ => {
					knoten.remove(0);
					continue;
				}
			};
			
			match aw {
				KrpcAntwort::FindNode { knoten_v4, knoten_v6 } => {
					// TODO zu kompliziert
					let knoten_res: Option<Vec<KnotenInfo<A>>> =
						if A::IST_IPV4 {
						knoten_v4.map(|v| v.into_iter().map(|k| KnotenInfo { 
							id: k.id,
							addr: A::aus_socket_addr(k.addr.into()).unwrap()
						}).collect())
					} else {
						knoten_v6.map(|v| v.into_iter().map(|k| KnotenInfo { 
							id: k.id,
							addr: A::aus_socket_addr(k.addr.into()).unwrap()
						}).collect())
					};
					if let Some(v) = knoten_res {
						knoten.extend_from_slice(v.as_slice());
					}
				},
				aw => {
					let m = aw.methode();
					log::debug!("knoten_iterativ_suchen: unpassende Antwort für find_nodes Anfrage: {m}");
				},
			}
			
			knoten.sort_by_key(|k| k.id ^ ziel);
		}
		
		let len = knoten.len();
		log::debug!("knoten_iterativ_suchen: {len} Treffer.");
		knoten
	}
}
