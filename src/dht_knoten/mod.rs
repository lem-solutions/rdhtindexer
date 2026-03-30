use crate::addr_generisch::Addr;
use crate::datentypen::{KnotenInfo, U160};
use bendy::decoding::Error as DeErr;
use bendy::encoding::ToBencode;
use metrics::*;
use smol::Timer;
use smol::io::Error as IoError;
use smol::net::{IpAddr, SocketAddr, UdpSocket};
use std::marker::{Send, Sync};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::{Mutex, RwLock};
use std::time::{Duration, Instant};

use crate::Fehler;
use crate::krpc::*;
use crate::tempomat::*;

mod anfragenpuffer;
mod bep42;
mod peer_tabelle;
mod routing_tabelle;
mod token;
pub use anfragenpuffer::Anfrageergebnis;
use anfragenpuffer::*;
use bep42::*;
use peer_tabelle::PeerTabelle;
use routing_tabelle::*;
use token::{token_generieren, token_überprüfen};

const VERSIONSCODE: Option<&'static [u8]> = None;

// https://bittorrent.org/beps/bep_0032.html
const MAX_KRPC_LEN: usize = 1024;

// Puffergröße für eingehende Pakete.
const MAX_UDP_LEN: usize = 2048;

const BEP51_INTERVAL_SEK: u16 = 300;

pub struct InfoHashMitKnoten {
	pub info_hash: U160,
	pub knoten_id: U160,
	pub addr: SocketAddr,
}

pub struct InfoHashesMitKnoten {
	pub info_hashes: Vec<U160>,
	pub knoten_id: U160,
	pub addr: SocketAddr,
}

pub struct DhtKnoten<A: Addr> {
	routing_tabelle: RwLock<RoutingTabelle<A>>,
	peer_tabelle: RwLock<PeerTabelle<A>>,
	ausstehende_anfragen: RwLock<Anfragenpuffer>,
	ext_ip_prüfer: Mutex<ExtIpPrüfer>,
	udp: UdpSocket,

	tempomat: Arc<Tempomat>,
	bei_announce_peer: Box<dyn Fn((U160, SocketAddr)) + Send + Sync>,
	bei_get_peers: Box<dyn Fn(InfoHashMitKnoten) + Send + Sync>,
	bei_eingehender_nachricht: Box<dyn Fn((U160, SocketAddr)) + Send + Sync>,

	anfragen_zeitgrenze: Duration,
	gestartet: AtomicBool,
}

impl<A: Addr> DhtKnoten<A> {
	// TODO Builder pattern o.Ä.
	pub async fn neu(
		addr: A,
		tempomat: Arc<Tempomat>,
		max_ausstehende_anfragen: usize,
		anfragen_zeitgrenze: Duration,
		max_peer_tabellen_größe: usize,
		max_peer_tabellen_alter: Duration,
		// die Funktionen dürfen nicht Blocken
		bei_announce_peer: Box<dyn Fn((U160, SocketAddr)) + Send + Sync>,
		bei_get_peers: Box<dyn Fn(InfoHashMitKnoten) + Send + Sync>,
		bei_eingehender_nachricht: Box<dyn Fn((U160, SocketAddr)) + Send + Sync>,
	) -> Result<Self, IoError> {
		Ok(DhtKnoten {
			routing_tabelle: RwLock::new(RoutingTabelle::neu()),
			// max_peers_pro_torrent: wir brauchen nur so viele wie in ein Paket passen.
			//                        16 ist definitiv genug und sollte problemlos passen.

			// Bei bep51_auswahl_größe gilt je mehr desdo besser solange es in ein Paket passt.
			// TODO: Maximal möglichen Wert ausrechnen und benutzen, beachten dass
			//       beim beantworten einer Anfrage die maximale Antwortgröße geringer
			//       sein kann. z. B. wegen einer längeren Transaktionsnummer.
			peer_tabelle: RwLock::new(PeerTabelle::neu(
				max_peer_tabellen_größe,
				16,
				max_peer_tabellen_alter,
				32,
				Duration::from_mins(30),
			)),
			ext_ip_prüfer: Mutex::new(ExtIpPrüfer::neu()),
			udp: UdpSocket::bind(addr).await?,
			tempomat,
			ausstehende_anfragen: RwLock::new(Anfragenpuffer::neu(
				max_ausstehende_anfragen,
			)),
			anfragen_zeitgrenze,
			gestartet: false.into(),
			bei_announce_peer,
			bei_get_peers,
			bei_eingehender_nachricht,
		})
	}

	pub fn starten(self: Arc<Self>, bootstrap_knoten: Vec<A>)
	where
		A: 'static,
	{
		let war_gestartet = self
			.gestartet
			.swap(true, std::sync::atomic::Ordering::Relaxed);
		assert!(!war_gestartet);

		let self_arc2 = self.clone();
		let self_arc3 = self.clone();
		let self_arc4 = self.clone();

		//let async_exec = smol::LocalExecutor::new();

		smol::spawn(self_arc2.nachrichten_empfangen()).detach();
		smol::spawn(self_arc3.routing_tabelle_warten()).detach();
		smol::spawn(self_arc4.anfragenpuffer_warten()).detach();

		for knoten_addr in bootstrap_knoten {
			let self_arc5 = self.clone();
			smol::spawn(self_arc5.bootstrap_ping(knoten_addr)).detach();
		}
	}

	async fn bootstrap_ping(self: Arc<Self>, knoten_addr: A) {
		let id = self.routing_tabelle.read().unwrap().eigene_id;
		self
			.anfrage_senden(
				KnotenInfo {
					id,
					addr: knoten_addr.clone(),
				},
				KrpcAnfrage::Ping,
				// Nicht wirklich eine Antwort
				Prio::Antworten,
				true,
			)
			.await
			.unwrap();
	}

	async fn anfragenpuffer_warten(self: Arc<Self>) -> Fehler {
		loop {
			if let Some(zeit) = {
				self
					.ausstehende_anfragen
					.write()
					.unwrap()
					.zeitgrenzen_überprüfen()
					.baldigste_zeitgrenze()
			} {
				Timer::at(zeit).await;
			} else {
				Timer::after(self.anfragen_zeitgrenze).await;
			}
		}
	}

	async fn routing_tabelle_warten(self: Arc<Self>) -> Result<(), Fehler> {
		loop {
			let k_info_opt = self
				.routing_tabelle
				.read()
				.unwrap()
				.fragwürdigen_knoten_finden()
				.cloned();
			if let Some(k_info) = k_info_opt {
				self.fragwürdigen_knoten_testen(k_info).await?;
			} else {
				// wenn es gerade keine fragwürdigen Knoten gibt warten wir einfach einwenig.
				smol::Timer::after(ROUTING_TABELLE_ZEITFENSTER / 2).await;
			}
		}
	}

	async fn fragwürdigen_knoten_testen(
		&self,
		k_info: routing_tabelle::KnotenInfo<A>,
	) -> Result<(), Fehler> {
		let ziel_id = k_info.id.clone();
		let ziel_addr = k_info.addr.clone();

		// Der Inhalt der Antwort interessiert uns nicht wirklich. Es ist nur
		// wichtig dass der Knoten gewantwortet hat. Die Routingdabelle wird
		// (wie bei jeder anderen Anfrage auch) automatisch über Erfolg oder
		// Miserfolg informiert, also müssen wir uns hier nicht weiter drum kümmern.
		self
			.anfrage_senden(
				KnotenInfo {
					id: ziel_id,
					addr: ziel_addr,
				},
				KrpcAnfrage::Ping,
				// Eig. nicht Antworten sondern Protokoll, aber ich glaube nicht das
				// wir dafür eine eigene Kategorie wollen.
				Prio::Antworten,
				true,
			)
			.await?
			.await?;

		Ok(())
	}

	async fn nachrichten_empfangen(self: Arc<Self>) -> Result<(), Fehler> {
		let mut puffer = [0u8; MAX_UDP_LEN + 1];
		loop {
			let (udp_len, quell_addr_enum) =
				self.udp.recv_from(&mut puffer[..]).await?;

			// TODO Wirklich unmöglich?
			let quell_addr = A::aus_socket_addr(quell_addr_enum).unwrap();

			self.tempomat.melden_runter(udp_len);
			if let Err(e) =
				self.paket_verarbeiten(quell_addr, &puffer[..udp_len]).await
			{
				log::debug!("{e}");
			}
		}
	}

	/// Verarbeitet ein eingehende UDP-Paket.
	///
	/// Da das eingehende Paket ungültig sein könnte sollten Fehler ignoriert
	/// werden.
	async fn paket_verarbeiten(
		&self,
		quell_addr: A,
		nachricht_bytes: &[u8],
	) -> Result<(), Fehler> {
		// Es gibt nichts das es nicht gibt, heißt aber nicht das es nicht Müll
		// ist ;)
		if quell_addr.port() == 0 {
			log::debug!("Paket von Port 0: {quell_addr} {:x?}", nachricht_bytes);
			return Err(Fehler::PaketVonPort0);
		}

		// Das UDP Paket könnte unvollständig sein, da wir nur MAX_UDP_LEN+1 Puffer
		// zur verfügung stellen, also ignorieren.
		if nachricht_bytes.len() > MAX_UDP_LEN {
			log::warn!(
				"UDP Paket zu groß ({} > {MAX_UDP_LEN})",
				nachricht_bytes.len()
			);
			return Err(Fehler::PaketZuGroß);
		}

		let nachricht = self.nachricht_deserialisieren(nachricht_bytes)?;
		let txnr = nachricht.transaktionsnummer.as_slice();

		match nachricht.inhalt {
			KrpcInhalt::Antwort { id, ext_ip, aw } => {
				self.antwort_verarbeiten(quell_addr, ext_ip, txnr, id, aw)
			}

			KrpcInhalt::Fehler(krpc_fehler) => {
				self.aw_fehler_verarbeiten(quell_addr, txnr, krpc_fehler)
			}

			KrpcInhalt::Anfrage { id, anf } => {
				self.anfrage_verarbeiten(quell_addr, txnr, id, anf).await
			}
		}
	}

	fn antwort_verarbeiten(
		&self,
		quell_addr: A,
		ext_addr_opt: Option<A>,
		txnr: &[u8],
		aw_id: U160,
		aw: KrpcAntwort,
	) -> Result<(), Fehler> {
		let methode = aw.methode();
		log::trace!("RX AW  {methode} {quell_addr} ");

		if let Some(ext_addr) = ext_addr_opt {
			if self
				.ext_ip_prüfer
				.lock()
				.unwrap()
				.ip_wechsel_prüfen(quell_addr.ip(), ext_addr.ip())
			{
				self.neue_ext_ip(ext_addr.ip());
			}
		}

		let anfrage_info = self
			.ausstehende_anfragen
			.write()
			.unwrap()
			.nehmen_bytes(txnr)
			.ok_or(Fehler::UnbekannteTransaktionsnummer)?;

		self
			.routing_tabelle
			.write()
			.unwrap()
			.antwort_erhalten(aw_id, quell_addr.clone());

		histogram!("Antwortlatenz").record(
			(anfrage_info.zeitgrenze - self.anfragen_zeitgrenze)
				.elapsed()
				.as_millis() as f64,
		);

		(self.bei_eingehender_nachricht)((aw_id, quell_addr.into()));

		// Ein Fehler beim Senden bedeutet das der Empfänger
		// nicht mehr existiert. In unserem Fall ist das nicht unbedingt ein
		// Fehler, falls wir uns nicht für die Antwort auf eine Anfrage
		// interessieren.
		#[allow(unused_must_use)]
		anfrage_info.sender.send(Anfrageergebnis::Ok(aw));
		Ok(())
	}

	fn aw_fehler_verarbeiten(
		&self,
		quell_addr: A,
		txnr: &[u8],
		f: KrpcFehler,
	) -> Result<(), Fehler> {
		let txt = &f.fehlermeldung;
		log::trace!("RX ERR {quell_addr} {txt}");

		let anfrage_info = self
			.ausstehende_anfragen
			.write()
			.unwrap()
			.nehmen_bytes(txnr)
			.ok_or(Fehler::UnbekannteTransaktionsnummer)?;

		if anfrage_info.bei_fehler_knoten_entfernen {
			self
				.routing_tabelle
				.write()
				.unwrap()
				.fehlschlag(anfrage_info.knoten_id);
		}

		// Ein Fehler beim Senden bedeutet das der Empfänger
		// nicht mehr existiert. In unserem Fall ist das nicht unbedingt ein
		// Fehler, falls wir uns nicht für die Antwort auf eine Anfrage
		// interessieren.
		#[allow(unused_must_use)]
		anfrage_info.sender.send(Anfrageergebnis::Fehler(f));
		Ok(())
	}

	fn nachricht_deserialisieren(
		&self,
		nachricht_bytes: &[u8],
	) -> Result<KrpcNachricht<A>, DeErr> {
		KrpcNachricht::einlesen(nachricht_bytes, |txid_bytes| {
			let txid = u16::from_be_bytes(txid_bytes.try_into().ok()?);
			self
				.ausstehende_anfragen
				.read()
				.unwrap()
				.methode_für_txid(txid as usize)
		})
	}

	async fn anfrage_verarbeiten(
		&self,
		quell_addr: A,
		txnr: &[u8],
		req_id: U160,
		anf: KrpcAnfrage,
	) -> Result<(), Fehler> {
		log::trace!("RX REQ {quell_addr}");

		if !quell_addr.global_valide() {
			return Err(Fehler::UngültigeAddresse(quell_addr.als_socket_addr()));
		}

		counter!("eingehende Anfragen").increment(1);

		(self.bei_eingehender_nachricht)((req_id, quell_addr.clone().into()));

		let aw = match anf {
			KrpcAnfrage::Ping => Ok(KrpcAntwort::Ping),
			KrpcAnfrage::FindNode { ziel, will } => {
				self.anfrage_bearbeiten_find_node(ziel, A::will_opt(will))
			}
			KrpcAnfrage::GetPeers { info_hash, will } => self
				.anfrage_bearbeiten_get_peers(
					info_hash,
					A::will_opt(will),
					&quell_addr,
					req_id,
				),
			KrpcAnfrage::AnnouncePeer {
				implizieter_port,
				info_hash,
				port,
				token,
			} => self.anfrage_bearbeiten_announce_peer(
				implizieter_port,
				info_hash,
				port,
				token,
				&quell_addr,
				req_id,
			),
			KrpcAnfrage::SampleInfohashes { ziel, will } => {
				self.anfrage_bearbeiten_sample_infohashes(ziel, A::will_opt(will))
			}
			KrpcAnfrage::UnbkannteMethode { name } => {
				let name_str = String::from_utf8_lossy(name.as_slice());
				log::debug!("Unbekannte Methode: {name_str}");
				Err(KrpcFehler {
					fehlercode: KrpcFehlercode::UnbekannteMethode,
					fehlermeldung: "unbekannte Mehtode".to_owned(),
				})
			}
		};

		match aw {
			Ok(msg) => {
				self
					.routing_tabelle
					.write()
					.unwrap()
					.anfrage_erhalten(req_id, quell_addr.clone());

				self.antwort_senden(&quell_addr, txnr, msg).await
			}
			Err(e) => self.fehler_senden(quell_addr, txnr, e).await,
		}
	}

	fn anfrage_bearbeiten_sample_infohashes(
		&self,
		ziel: U160,
		will: Will,
	) -> Result<KrpcAntwort, KrpcFehler> {
		let (knoten_v4, knoten_v6) = match self.nächste_knoten(ziel, will)? {
			KrpcAntwort::FindNode {
				knoten_v4: v4,
				knoten_v6: v6,
			} => (v4, v6),
			_ => unreachable!(),
		};

		let mut peer_tabelle_ref = self.peer_tabelle.write().unwrap();
		// Das die Hashliste nicht zu groß sein kann liegt in der Verantwortung der `PeerTabelle`.
		let hashes = peer_tabelle_ref.bep51_anfrage().clone();
		let ges_anz = peer_tabelle_ref.anz_torrents();
		std::mem::drop(peer_tabelle_ref);

		Ok(KrpcAntwort::SampleInfohashes {
			interval_sek: Some(BEP51_INTERVAL_SEK),
			knoten_v4,
			knoten_v6,
			info_hashes: hashes.clone(),
			anz_infohashes: Some(ges_anz),
		})
	}

	fn anfrage_bearbeiten_announce_peer(
		&self,
		implizierter_port: bool,
		info_hash: U160,
		port: u16,
		token: Vec<u8>,
		quell_addr: &A,
		req_id: U160,
	) -> Result<KrpcAntwort, KrpcFehler> {
		if !token_überprüfen(req_id, quell_addr, token.as_slice()) {
			return Err(KrpcFehler {
				fehlercode: KrpcFehlercode::ProtokollFehler,
				fehlermeldung: "fehlerhaftes Token".to_owned(),
			});
		}

		let peer_addr = if implizierter_port {
			quell_addr.clone()
		} else {
			let mut a = quell_addr.clone();
			a.port_ändern(port);
			a
		};

		(self.bei_announce_peer)((info_hash, peer_addr.clone().into()));

		self
			.peer_tabelle
			.write()
			.unwrap()
			.peer_einfügen(info_hash, peer_addr);

		Ok(KrpcAntwort::AnnouncePeer)
	}

	fn anfrage_bearbeiten_get_peers<'a>(
		&self,
		info_hash: U160,
		will: Will,
		quell_addr: &A,
		req_id: U160,
	) -> Result<KrpcAntwort, KrpcFehler> {
		(self.bei_get_peers)(InfoHashMitKnoten {
			info_hash,
			knoten_id: req_id,
			addr: quell_addr.clone().into(),
		});

		let mut peer_tabelle = self.peer_tabelle.write().unwrap();
		let peer_iter = peer_tabelle.peers_für_hash(&info_hash);
		let token = token_generieren(req_id, quell_addr).to_vec();
		if peer_iter.len() == 0 {
			let (knoten_v4, knoten_v6) =
				match self.nächste_knoten(info_hash, will)? {
					KrpcAntwort::FindNode {
						knoten_v4,
						knoten_v6,
					} => (knoten_v4, knoten_v6),
					_ => unreachable!(),
				};
			Ok(KrpcAntwort::GetPeers {
				peers: None,
				knoten_v4,
				knoten_v6,
				token,
			})
		} else {
			Ok(KrpcAntwort::GetPeers {
				peers: Some(peer_iter.map(|a| a.clone().into()).collect()),
				knoten_v4: None,
				knoten_v6: None,
				token,
			})
		}
	}

	fn nächste_knoten(
		&self,
		ziel: U160,
		will: Will,
	) -> Result<KrpcAntwort, KrpcFehler> {
		let rt_b = self.routing_tabelle.read().unwrap();
		let puffer = rt_b.nächste_k_knoten(ziel);

		let knoten_v4 = if A::IST_IPV4 && will.v4() {
			Some(
				puffer
					.iter()
					.map(|(knoten_id, addr)| KnotenInfo {
						id: *knoten_id,
						addr: addr.als_ipv4().unwrap().clone(),
					})
					.collect(),
			)
		} else if will.v4() {
			Some(Vec::new())
		} else {
			None
		};

		let knoten_v6 = if !A::IST_IPV4 && will.v6() {
			Some(
				puffer
					.iter()
					.map(|(knoten_id, addr)| KnotenInfo {
						id: *knoten_id,
						addr: addr.als_ipv6().unwrap().clone(),
					})
					.collect(),
			)
		} else if will.v6() {
			Some(Vec::new())
		} else {
			None
		};

		Ok(KrpcAntwort::FindNode {
			knoten_v4,
			knoten_v6,
		})
	}

	fn anfrage_bearbeiten_find_node<'a>(
		&self,
		ziel: U160,
		will: Will,
	) -> Result<KrpcAntwort, KrpcFehler> {
		self.nächste_knoten(ziel, will)
	}

	async fn nachricht_abschicken<'a>(
		&self,
		nachricht: &KrpcNachricht<A>,
		ziel: &A,
		priorität: Prio,
	) -> Result<usize, Fehler> {
		let datagramm = nachricht.to_bencode()?;
		if datagramm.len() > MAX_KRPC_LEN {
			return Err(Fehler::GesendeteNachrichtZuLang(datagramm.len()));
		}

		self.tempomat.warten_hoch(priorität, datagramm.len()).await;

		let anz_geschrieben = self
			.udp
			.send_to(&datagramm[..], ziel.als_socket_addr())
			.await?;
		if anz_geschrieben != datagramm.len() {
			log::warn!(
				"UDP Datagramm Längenfehler (Puffer: {}, gesendet: {})",
				datagramm.len(),
				anz_geschrieben
			);
		}

		Ok(anz_geschrieben)
	}

	async fn antwort_senden<'a>(
		&self,
		ziel: &A,
		tx_nummer: &'a [u8],
		aw: KrpcAntwort,
	) -> Result<(), Fehler> {
		let m = &aw.methode();
		log::trace!("TX AW  {m} {ziel}");
		let n = KrpcNachricht {
			transaktionsnummer: tx_nummer.to_vec(),
			versionscode: VERSIONSCODE.map(|v| v.to_vec()),
			inhalt: KrpcInhalt::Antwort {
				id: self.routing_tabelle.read().unwrap().eigene_id,
				ext_ip: Some(ziel.clone()),
				aw,
			},
		};

		self.nachricht_abschicken(&n, ziel, Prio::Antworten).await?;
		counter!("ausgehende Antworten").increment(1);
		Ok(())
	}

	async fn fehler_senden<'a>(
		&self,
		ziel: A,
		tx_nummer: &'a [u8],
		krpc_fehler: KrpcFehler,
	) -> Result<(), Fehler> {
		let txt = &krpc_fehler.fehlermeldung;
		log::trace!("TX ERR {ziel} {txt}");
		let n = KrpcNachricht {
			transaktionsnummer: tx_nummer.to_vec(),
			versionscode: VERSIONSCODE.map(|v| v.to_vec()),
			inhalt: KrpcInhalt::Fehler(krpc_fehler),
		};

		self
			.nachricht_abschicken(&n, &ziel, Prio::Antworten)
			.await?;

		counter!("ausgehende Fehler").increment(1);
		Ok(())
	}

	pub async fn anfrage_senden<'a>(
		&self,
		ziel: KnotenInfo<A>,
		anf: KrpcAnfrage,
		priorität: Prio,
		bei_fehler_knoten_entfernen: bool,
	) -> Result<oneshot::Receiver<Anfrageergebnis>, Fehler> {
		let anf_methode = anf.methode();
		let (aw_sender, aw_empf) = oneshot::channel();
		let tx_nummer = Anfragenpuffer::einfügen(
			|| self.ausstehende_anfragen.write().unwrap(),
			anf_methode.clone(),
			ziel.id,
			self.anfragen_zeitgrenze,
			bei_fehler_knoten_entfernen,
			aw_sender,
		)
		.await;

		let n = KrpcNachricht {
			transaktionsnummer: tx_nummer.to_vec(),
			versionscode: VERSIONSCODE.map(|v| v.to_vec()),
			inhalt: KrpcInhalt::Anfrage {
				id: self.routing_tabelle.read().unwrap().eigene_id,
				anf,
			},
		};

		let ziel_addr = &ziel.addr;
		counter!("ausgehende Anfragen").increment(1);
		log::trace!("TX REQ {anf_methode} {ziel_addr}");
		// Da eine Antwort kommen wird müssen wir ggf. auch für Runter warten.
		self.tempomat.warten_runter(priorität, 0).await;
		self.nachricht_abschicken(&n, &ziel.addr, priorität).await?;
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
		sender: smol::channel::Sender<KnotenInfo<A>>,
		priorität: Prio,
	) -> Vec<KnotenInfo<A>> {
		let mut knoten: Vec<KnotenInfo<A>> = Vec::new();
		{
			let rt_g = self.routing_tabelle.read().unwrap();
			{
				let knoten_minivec = rt_g.nächste_k_knoten(ziel);

				for (id, addr) in knoten_minivec.iter() {
					knoten.push(KnotenInfo {
						id: *id,
						addr: (*addr).clone(),
					});
				}
			}
		}
		knoten.sort_by_key(|k| k.id ^ ziel);
		if knoten.is_empty() {
			return knoten;
		}

		let mut beste_entfernung = U160([255; 20]);

		while beste_entfernung > knoten[0].id ^ ziel {
			if knoten.is_empty() {
				return knoten;
			}
			let anf = KrpcAnfrage::FindNode { ziel, will: None };
			let r1 = self
				.anfrage_senden(knoten[0].clone(), anf, priorität, true)
				.await;

			let r2 = match r1 {
				Ok(a) => a,
				Err(err) => {
					log::warn!("Fehler bei knoten_iterativ_suchen: {err}");
					return knoten;
				}
			};

			// unwrap: Kann nur Err sein wenn der Sender weg ist.
			//         Das sollte nie passieren.
			let aw = match r2.await.unwrap() {
				Anfrageergebnis::Ok(aw) => aw,
				erg => {
					log::trace!("knoten_iterativ_suchen: ERG {:?}", erg);
					knoten.remove(0);
					if knoten.is_empty() {
						return knoten;
					}
					continue;
				}
			};

			match aw {
				KrpcAntwort::FindNode {
					knoten_v4,
					knoten_v6,
				} => {
					// TODO zu kompliziert
					let knoten_res: Option<Vec<KnotenInfo<A>>> = if A::IST_IPV4 {
						knoten_v4.map(|v| {
							v.into_iter()
								.map(|k| KnotenInfo {
									id: k.id,
									addr: A::aus_socket_addr(k.addr.into()).unwrap(),
								})
								.collect()
						})
					} else {
						knoten_v6.map(|v| {
							v.into_iter()
								.map(|k| KnotenInfo {
									id: k.id,
									addr: A::aus_socket_addr(k.addr.into()).unwrap(),
								})
								.collect()
						})
					};
					if let Some(v) = knoten_res {
						beste_entfernung = knoten[0].id ^ ziel;
						knoten.extend_from_slice(v.as_slice());
						for k in v {
							sender.send(k).await.unwrap();
						}
					}
				}
				aw => {
					let m = aw.methode();
					log::debug!(
						"knoten_iterativ_suchen: unpassende Antwort für find_nodes Anfrage: {m}"
					);
				}
			}

			knoten.sort_by_key(|k| k.id ^ ziel);
		}

		let len = knoten.len();
		log::debug!("knoten_iterativ_suchen: {len} Treffer.");
		counter!("knoten_iterativ_suchen anz Knoten")
			.increment(len.try_into().unwrap());
		counter!("knoten_iterativ_suchen abgeschlossen").increment(1);
		knoten
	}

	fn neue_ext_ip(&self, ext_ip: IpAddr) {
		if self
			.routing_tabelle
			.read()
			.unwrap()
			.eigene_id
			.bep42_prüfen(&ext_ip)
		{
			return;
		}

		self
			.routing_tabelle
			.write()
			.unwrap()
			.id_ändern(U160::bep42_generieren(&ext_ip));

		// Wir haben wahrscheinlich immernoch Infohashes nahe der alten Knoten-ID
		// in der Peer-Tabelle, da diese durch neuere verdängt werden können und
		// kein Nachteil daran besteht ferne Infohashes zu haben brauchen wir diese
		// nicht zurück zu setzten.
	}
}
