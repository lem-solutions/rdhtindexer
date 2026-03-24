use crate::datentypen::{U160, KnotenInfo};
use crate::addr_generisch::Addr;

use smol::net::{SocketAddrV4, SocketAddrV6, SocketAddr};
use bendy::decoding::*;
use bendy::encoding::*;
use bendy::decoding::Error as DeErr;
use bendy::encoding::Error as EnErr;

/// KRPC Datenstrukturen mit (De)Serialisierungsmethoden.
/// Eine `serde`-Basierte implementierung hat sich aufgrund einiger Eigenheiten
/// des DHT-Protokolls as ungeeignet erwiesen.

// TODO Fehlermeldungen sind nicht sauber(Wir benutzen `DeErr::unexpected_token` statt `DeErr::malformed_content`)

// TODO starke Codeduplikation bei den *_argumente_einlesen Funktionen vermeiden und generell refactoren.


/// Liest ein Bencode-Dict ein und fürht für alle genfundenen Schlüssel einen
/// Befehl mit dem entsprechenden Wert als Argument aus. Der Rückgabewert
/// des Befehls wird mit dem `?`-Operator verarbeitet, sollte also je nach
/// Rückgabewert der aktuellen Funktion z. B. ein `Result` sein.
/// 
/// Es wird für jeden gesuchten Schlüssel eine Variable des Typs `Option<T>` defniniert.
/// Sollte der Schlüssel existieren ist der Wert `Some(/* ... */)` mit dem
/// Rüggabewert des Befehls(nach Anwandung des `?`-Operators), ansonsten `None`.
/// 
/// Syntax:
/// ```
/// dict_einlesen!(zu_verarbeitendes_dict,
/// 	b"gesuchter Schlüssel" => Varaiblenname: Befehl,
/// 	b"anderer gesuchter Schlüssel" => Varaiblenname2: Befehl2
/// );
/// ```
/// 
/// Hinweise:
/// - Das dict muss als `&mut DictDecoder` zur Verfügung gestellt werden.
/// - Nach Auführung des Befehls darf das Argument nicht ausgeliehen bleiben.
macro_rules! dict_einlesen {
	($dict:expr, $( $schlüssel:literal => $name:ident : $befehl:expr ),+) => {
		$( let mut $name = None;)+
		while let Some((n, w)) = $dict.next_pair()? {
			match n {
				$($schlüssel => $name = Some($befehl(w)?),)+
				_ => {}
			}
		}
	}
}

/// Sucht nach einem Schlüssel in einem Dict und führt die Funktion `f` mit dem
/// Wert aus, sollte der Schlüssel gefunden werden.
/// 
/// Die API muss wegen Unzulänglichkeiten des Borrowcheckers so sein.
/// Siehe <https://docs.rs/polonius-the-crab/latest/polonius_the_crab/>
fn element_suchen<'obj, 'ser: 'obj, T, F: FnOnce(Object) -> T>(
	mut dict: DictDecoder<'obj, 'ser>,
	schlüssel: &'static [u8],
	f: F,
) -> Result<Option<T>, DeErr> {
	loop {
		match dict.next_pair()? {
			Some((s, w)) if s == schlüssel => break Ok(Some(f(w))),
			Some(_) => {},
			None => break Ok(None),
		}
	}
}

pub struct KrpcNachricht<A: Addr> {
	pub transaktionsnummer: Vec<u8>,
	pub versionscode : Option<Vec<u8>>,
	pub inhalt: KrpcInhalt<A>,
}
impl<A: Addr> ToBencode for KrpcNachricht<A> {
	const MAX_DEPTH: usize = 16;
	
	fn encode(&self, en: SingleItemEncoder) -> Result<(), EnErr> {
		en.emit_unsorted_dict(|e_haupt| {
			e_haupt.emit_pair_with(b"t", |e| e.emit_bytes(self.transaktionsnummer.as_slice()))?;
			if let Some(v) = self.versionscode.as_ref() {
				e_haupt.emit_pair_with(b"v", |e| e.emit_bytes(v.as_slice()))?;
			}
			match self.inhalt {
				KrpcInhalt::Anfrage { id, ref anf } => {
					e_haupt.emit_pair(b"y", "q")?;
					e_haupt.emit_pair(b"q", anf.methode())?;
					e_haupt.emit_pair_with(b"a",|e_a| {
						e_a.emit_unsorted_dict(|mut e_arg| {
							e_arg.emit_pair(b"id", id)?;
							anf.argumente_ausgeben(&mut e_arg)
						})
					})?;
				},
				KrpcInhalt::Antwort { id, ref ext_ip, ref aw } => {
					e_haupt.emit_pair(b"y", "r")?;
					if let Some(ip) = ext_ip {
						let mut puffer = vec![0u8;A::KRPC_LEN];
						ip.als_krpc_bytes(puffer.as_mut_slice());
						e_haupt.emit_pair_with(b"ip", |e| e.emit_bytes(puffer.as_slice()))?;
					}
					e_haupt.emit_pair_with(b"r",|e_r| {
						e_r.emit_unsorted_dict(|mut e_arg| {
							e_arg.emit_pair(b"id", id)?;
							aw.argumente_ausgeben(&mut e_arg)
						})
					})?;
				},
				KrpcInhalt::Fehler(ref f) => {
					e_haupt.emit_pair(b"y", "e")?;
					e_haupt.emit_pair(b"e", f)?;
				}
			}
			
			Ok(())
		})
	}
}
impl<A: Addr> KrpcNachricht<A> {
	/// Liest das KRPC-Paket ein.
	/// 
	/// Für Antworten ist es nötig die Methode der entsprechenden Anfrage zu kennen.
	/// `methode_für_txid` muss für eine gegebene Transaktionsnummer die
	/// entsprechende Methode zurückgeben.
	/// 
	/// TODO auch wenn die Reihenfolge einge gewisse komplexität erzwingt, sollte
	///      es auch übersichtlicher gehen als so.
	/// 
	/// TODO Wenn `methode_für_txid` `None` zurückgibt können wir das Paket nicht
	///      dekodieren. Das ist nicht wirklich ein `DeErr` und wir sollten nicht
	///      so tun als wäre es einer.
	pub fn einlesen<F: FnOnce(&[u8]) -> Option<AnfrageMethode>>(daten: &[u8], methode_für_txid: F) -> Result<Self, DeErr> {
		let mut dekodierer = bendy::decoding::Decoder::new(daten).with_max_depth(16);
		let mut hauptobjekt = dekodierer.next_object()?
			.ok_or(DeErr::unexpected_token("Haupt-Dict", "Nichts"))?
			.try_into_dictionary()?;
		
		dict_einlesen!(
			&mut hauptobjekt,
			b"y" => nachricht_typ: |w: Object| match w.try_into_bytes()? {
				b"q" => Ok(NachrichtTyp::Anfrage),
				b"r" => Ok(NachrichtTyp::Antwort),
				b"e" => Ok(NachrichtTyp::Fehler),
				wert_bytes => Err(DeErr::unexpected_token("q, r oder e", String::from_utf8_lossy(wert_bytes))),
			},
			b"v" => versionscode: |w: Object| w.try_into_bytes().map(|v| v.to_vec()),
			b"t" => transaktionsnummer: |w: Object| w.try_into_bytes().map(|v| v.to_vec()),
			b"e" => fehler: KrpcFehler::decode_bencode_object,
			b"q" => anfrage_methode: AnfrageMethode::decode_bencode_object,
			b"ip" => ext_ip: |w: Object| A::aus_krpc_bytes(w.try_into_bytes()?).ok_or(DeErr::unexpected_token("Kompakte IP+Port", "???"))
		);
		std::mem::drop(hauptobjekt);
		std::mem::drop(dekodierer);
		
		if fehler.is_some() && nachricht_typ != Some(NachrichtTyp::Fehler) {
			return Err(DeErr::unexpected_token("Haupt-Dict enthält „e“", "obwohl y != e"));
		}
		
		match nachricht_typ {
			Some(NachrichtTyp::Anfrage) => {
				let mut dekodierer2 = bendy::decoding::Decoder::new(daten).with_max_depth(16);
				let hauptobjekt2 = dekodierer2.next_object()?
					.ok_or(DeErr::unexpected_token("Haupt-Dict", "Nichts"))?
					.try_into_dictionary()?;
				
				// Dreifach verschachtelter Datentyp wegen drei verschiedenen Fehlerquellen.
				// 
				//           | `Err` von `DictDecoder::next_pair` aus `element_suchen`
				//           |      | `None` wenn der Schlüssel „a“ fehlt.
				//           v      v      v Rückgabe von KrpcAnfrage::*_argumente_einlesen
				// Datentyp: Result<Option<Result<KrpcInhalt<A>>>>
				let krpc_inhalt_res = match anfrage_methode.ok_or(DeErr::missing_field("q"))? {
					AnfrageMethode::Ping => element_suchen(hauptobjekt2, b"a", KrpcAnfrage::ping_argumente_einlesen),
					AnfrageMethode::FindNode => element_suchen(hauptobjekt2, b"a", KrpcAnfrage::find_node_argumente_einlesen),
					AnfrageMethode::GetPeers => element_suchen(hauptobjekt2, b"a", KrpcAnfrage::get_peers_argumente_einlesen),
					AnfrageMethode::AnnouncePeer => element_suchen(hauptobjekt2, b"a", KrpcAnfrage::announce_peer_argumente_einlesen),
					AnfrageMethode::SampleInfohashes => element_suchen(hauptobjekt2, b"a", KrpcAnfrage::sample_infohashes_argumente_einlesen),
					AnfrageMethode::Unbekannt(name) => element_suchen(hauptobjekt2, b"a", |a| KrpcAnfrage::unbekannte_methode_argumente_einlesen(a, name)),
				};
				
				let krpc_inhalt = krpc_inhalt_res?
					.ok_or(DeErr::missing_field("a"))??;
				
				Ok(KrpcNachricht {
					transaktionsnummer: transaktionsnummer.ok_or(DeErr::missing_field("t"))?.to_vec(),
					versionscode: versionscode.map(|v| v.to_vec()),
					inhalt: krpc_inhalt,
				})
			},
			Some(NachrichtTyp::Antwort) => {
				let mut dekodierer2 = bendy::decoding::Decoder::new(daten).with_max_depth(16);
				let hauptobjekt2 = dekodierer2.next_object()?
					.ok_or(DeErr::unexpected_token("Haupt-Dict", "Nichts"))?
					.try_into_dictionary()?;
				
				// Dreifach verschachtelter Datentyp wegen drei verschiedenen Fehlerquellen.
				// 
				//           | `Err` von `DictDecoder::next_pair` aus `element_suchen`
				//           |      | `None` wenn der Schlüssel „a“ fehlt.
				//           v      v      v Rückgabe von KrpcAntwort::*_argumente_einlesen
				// Datentyp: Result<Option<Result<KrpcInhalt<A>>>>
				let krpc_inhalt_res = match methode_für_txid(transaktionsnummer.as_ref().ok_or(DeErr::missing_field("t"))?.as_slice())
					// *würg*
					.ok_or(DeErr::unexpected_token("Bekannte Tranaktionsnummer", "Unbekannte Tranaktionsnummer"))?
				{
					AnfrageMethode::Ping => element_suchen(hauptobjekt2, b"r", |a| KrpcAntwort::ping_argumente_einlesen(a, ext_ip)),
					AnfrageMethode::FindNode => element_suchen(hauptobjekt2, b"r", |a| KrpcAntwort::find_node_argumente_einlesen(a, ext_ip)),
					AnfrageMethode::GetPeers => element_suchen(hauptobjekt2, b"r", |a| KrpcAntwort::get_peers_argumente_einlesen(a, ext_ip)),
					AnfrageMethode::AnnouncePeer => element_suchen(hauptobjekt2, b"r", |a| KrpcAntwort::announce_peer_argumente_einlesen(a, ext_ip)),
					AnfrageMethode::SampleInfohashes => element_suchen(hauptobjekt2, b"r", |a| KrpcAntwort::sample_infohashes_argumente_einlesen(a, ext_ip)),
					// Wir können keine Anfrage mit Unbekannter Methode abgeschickt haben.
					// Das wäre auf jeden Fall ein Bug, deswegen panic.
					AnfrageMethode::Unbekannt(name) => panic!("methode_für_txid hat AnfrageMethode::Unbekannt({name:?}) zurückgegeben"),
				};
				
				let krpc_inhalt = krpc_inhalt_res?
					.ok_or(DeErr::missing_field("a"))??;
				
				Ok(KrpcNachricht {
					transaktionsnummer: transaktionsnummer.ok_or(DeErr::missing_field("t"))?.to_vec(),
					versionscode: versionscode.map(|v| v.to_vec()),
					inhalt: krpc_inhalt,
				})
			},
			Some(NachrichtTyp::Fehler) => Ok(KrpcNachricht {
				transaktionsnummer: transaktionsnummer.ok_or(DeErr::missing_field("t"))?.to_vec(),
				versionscode: versionscode.map(|v| v.to_vec()),
				inhalt: KrpcInhalt::Fehler(fehler.ok_or(DeErr::missing_field("e"))?),
			}),
			None => Err(DeErr::missing_field("y")),
		}
	}
}

#[derive(PartialEq)]
pub enum NachrichtTyp {
	Anfrage,
	Antwort,
	Fehler,
}

#[derive(Clone)]
pub enum AnfrageMethode {
	Ping,
	FindNode,
	GetPeers,
	AnnouncePeer,
	SampleInfohashes,
	Unbekannt(Vec<u8>),
}
impl std::fmt::Display for AnfrageMethode {
	fn fmt(&self, fmt: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
		match self {
			AnfrageMethode::Ping => write!(fmt, "Ping"),
			AnfrageMethode::FindNode => write!(fmt, "FindNode"),
			AnfrageMethode::GetPeers => write!(fmt, "GetPeers"),
			AnfrageMethode::AnnouncePeer => write!(fmt, "AnnouncePeer"),
			AnfrageMethode::SampleInfohashes => write!(fmt, "SampleInfohashes"),
			AnfrageMethode::Unbekannt(v) => 
				write!(fmt, "{}", String::from_utf8_lossy(v)),
		}
	}
}

impl ToBencode for AnfrageMethode {
	const MAX_DEPTH: usize = 0;
	
	fn encode(&self, en: SingleItemEncoder) -> Result<(), EnErr> {
		match self {
			AnfrageMethode::Ping => en.emit_str("ping"),
			AnfrageMethode::FindNode => en.emit_str("find_node"),
			AnfrageMethode::GetPeers => en.emit_str("get_peers"),
			AnfrageMethode::AnnouncePeer => en.emit_str("announce_peer"),
			AnfrageMethode::SampleInfohashes => en.emit_str("sample_infohashes"),
			AnfrageMethode::Unbekannt(m) => en.emit_bytes(m.as_slice()),
		}
	}
}
impl FromBencode for AnfrageMethode {
	const EXPECTED_RECURSION_DEPTH: usize = 0;
	
	fn decode_bencode_object(obj: Object) -> Result<Self, DeErr>
		where Self: Sized
	{
		Ok(match obj.try_into_bytes()? {
			b"ping" => AnfrageMethode::Ping,
			b"find_node" => AnfrageMethode::FindNode,
			b"get_peers" => AnfrageMethode::GetPeers,
			b"announce_peer" => AnfrageMethode::AnnouncePeer,
			b"sample_infohashes" => AnfrageMethode::SampleInfohashes,
			m => AnfrageMethode::Unbekannt(m.to_vec())
		})
	}
}

pub enum KrpcInhalt<A: Addr> {
	Anfrage{id: U160, anf: KrpcAnfrage},
	Antwort{id: U160, ext_ip: Option<A>, aw: KrpcAntwort},
	Fehler(KrpcFehler),
}

pub enum KrpcAnfrage {
	Ping,
	FindNode{ziel: U160, will: Option<Will>},
	GetPeers{info_hash: U160, will: Option<Will>},
	AnnouncePeer{
		implizieter_port: bool,
		info_hash: U160,
		port: u16,
		token: Vec<u8>,
	},
	SampleInfohashes{ziel: U160, will: Option<Will>},
	UnbkannteMethode{name: Vec<u8>},
}
impl KrpcAnfrage {
	pub fn methode(&self) -> AnfrageMethode {
		match self {
			KrpcAnfrage::Ping => AnfrageMethode::Ping,
			KrpcAnfrage::FindNode { .. } => AnfrageMethode::FindNode,
			KrpcAnfrage::GetPeers { .. } => AnfrageMethode::GetPeers,
			KrpcAnfrage::AnnouncePeer { .. } => AnfrageMethode::AnnouncePeer,
			KrpcAnfrage::SampleInfohashes { .. } => AnfrageMethode::SampleInfohashes,
			KrpcAnfrage::UnbkannteMethode { name } => AnfrageMethode::Unbekannt(name.clone()),
		}
	}
	
	fn argumente_ausgeben(&self, en: &mut UnsortedDictEncoder) -> Result<(), EnErr> {
		match self {
			KrpcAnfrage::Ping => {},
			KrpcAnfrage::FindNode { ziel, will } => {
				en.emit_pair(b"target", ziel)?;
				if let Some(w) = will { en.emit_pair(b"want", w)?; }
			},
			KrpcAnfrage::GetPeers { info_hash, will } => {
				en.emit_pair(b"info_hash", info_hash)?;
				if let Some(w) = will { en.emit_pair(b"want", w)?; }
			},
			KrpcAnfrage::AnnouncePeer { implizieter_port, info_hash, port, token } => {
				if *implizieter_port { en.emit_pair(b"implied_port", 1)?; }
				en.emit_pair(b"info_hash", info_hash)?;
				en.emit_pair(b"port", port)?;
				en.emit_pair_with(b"token", |e| e.emit_bytes(token.as_slice()))?;
			},
			KrpcAnfrage::SampleInfohashes { ziel, will } => {
				en.emit_pair(b"target", ziel)?;
				if let Some(w) = will { en.emit_pair(b"want", w)?; }
			},
			KrpcAnfrage::UnbkannteMethode {..} => panic!("Es wurde versucht eine `KrpcAnfage` mit Unbekannter Methode zu kodieren"),
		}
		
		Ok(())
	}
	
	fn ping_argumente_einlesen<A: Addr>(obj: Object) -> Result<KrpcInhalt<A>, DeErr> {
		let mut d = obj.try_into_dictionary()?;
		dict_einlesen!(&mut d, b"id" => id: U160::decode_bencode_object);
		Ok(KrpcInhalt::Anfrage {
			id: id.ok_or(DeErr::missing_field("id"))?,
			anf: KrpcAnfrage::Ping
		})
	}
	
	fn find_node_argumente_einlesen<A: Addr>(obj: Object) -> Result<KrpcInhalt<A>, DeErr> {
		let mut d = obj.try_into_dictionary()?;
		dict_einlesen!(&mut d,
			b"id" => id: U160::decode_bencode_object,
			b"target" => ziel: U160::decode_bencode_object,
			b"want" => will: Will::decode_bencode_object
		);
		Ok(KrpcInhalt::Anfrage {
			id: id.ok_or(DeErr::missing_field("id"))?,
			anf: KrpcAnfrage::FindNode { ziel: ziel.ok_or(DeErr::missing_field("target"))?, will}
		})
	}
	
	fn get_peers_argumente_einlesen<A: Addr>(obj: Object) -> Result<KrpcInhalt<A>, DeErr> {
		let mut d = obj.try_into_dictionary()?;
		dict_einlesen!(&mut d,
			b"id" => id: U160::decode_bencode_object,
			b"info_hash" => info_hash: U160::decode_bencode_object,
			b"want" => will: Will::decode_bencode_object
		);
		Ok(KrpcInhalt::Anfrage {
			id: id.ok_or(DeErr::missing_field("id"))?,
			anf: KrpcAnfrage::GetPeers { info_hash: info_hash.ok_or(DeErr::missing_field("target"))?, will}
		})
	}
	
	fn announce_peer_argumente_einlesen<A: Addr>(obj: Object) -> Result<KrpcInhalt<A>, DeErr> {
		let mut d = obj.try_into_dictionary()?;
		dict_einlesen!(&mut d,
			b"id" => id: U160::decode_bencode_object,
			b"implied_port" => implizieter_port: implizieten_port_dekodieren,
			b"info_hash" => info_hash: U160::decode_bencode_object,
			b"port" => port: |w: Object| Ok::<_, DeErr>(w.try_into_integer()?.parse().map_err(|e| DeErr::malformed_content(e))?),
			b"token" => token: |w: Object| w.try_into_bytes().map(|v| v.to_vec())
		);
		Ok(KrpcInhalt::Anfrage {
			id: id.ok_or(DeErr::missing_field("id"))?,
			anf: KrpcAnfrage::AnnouncePeer {
				implizieter_port: implizieter_port.unwrap_or(false),
				info_hash: info_hash.ok_or(DeErr::missing_field("info_hash"))?,
				port: port.ok_or(DeErr::missing_field("port"))?,
				token: token.ok_or(DeErr::missing_field("token"))?,
			}
		})
	}
	
	fn sample_infohashes_argumente_einlesen<A: Addr>(obj: Object) -> Result<KrpcInhalt<A>, DeErr> {
		let mut d = obj.try_into_dictionary()?;
		dict_einlesen!(&mut d,
			b"id" => id: U160::decode_bencode_object,
			b"target" => ziel: U160::decode_bencode_object,
			b"want" => will: Will::decode_bencode_object
		);
		Ok(KrpcInhalt::Anfrage {
			id: id.ok_or(DeErr::missing_field("id"))?,
			anf: KrpcAnfrage::SampleInfohashes {
				ziel: ziel.ok_or(DeErr::missing_field("info_hash"))?,
				will,
			}
		})
	}

	fn unbekannte_methode_argumente_einlesen<A: Addr>(obj: Object, name: Vec<u8>) -> Result<KrpcInhalt<A>, DeErr> {
		let mut d = obj.try_into_dictionary()?;
		dict_einlesen!(&mut d, b"id" => id: U160::decode_bencode_object);
		Ok(KrpcInhalt::Anfrage {
			id: id.ok_or(DeErr::missing_field("id"))?,
			anf: KrpcAnfrage::UnbkannteMethode { name },
		})
	}
}

pub enum KrpcAntwort {
	Ping,
	FindNode{
		knoten_v4: Option<Vec<KnotenInfo<SocketAddrV4>>>,
		knoten_v6: Option<Vec<KnotenInfo<SocketAddrV6>>>,
	},
	GetPeers{
		peers: Option<Vec<SocketAddr>>, // Kann eine hybride Liste sein
		knoten_v4: Option<Vec<KnotenInfo<SocketAddrV4>>>,
		knoten_v6: Option<Vec<KnotenInfo<SocketAddrV6>>>,
		token: Vec<u8>,
	},
	AnnouncePeer,
	SampleInfohashes{
		interval_sek: u16,
		knoten_v4: Option<Vec<KnotenInfo<SocketAddrV4>>>,
		knoten_v6: Option<Vec<KnotenInfo<SocketAddrV6>>>,
		anz_infohashes: usize,
		info_hashes: Vec<U160>,
	}
}
impl KrpcAntwort {
	pub fn methode(&self) -> AnfrageMethode {
		match self {
			KrpcAntwort::Ping => AnfrageMethode::Ping,
			KrpcAntwort::FindNode { .. } => AnfrageMethode::FindNode,
			KrpcAntwort::GetPeers { .. } => AnfrageMethode::GetPeers,
			KrpcAntwort::AnnouncePeer => AnfrageMethode::AnnouncePeer,
			KrpcAntwort::SampleInfohashes { .. } => AnfrageMethode::SampleInfohashes,
		}
	}
	
	fn argumente_ausgeben(&self, en: &mut UnsortedDictEncoder) -> Result<(), EnErr> {
		match self {
			KrpcAntwort::Ping => {},
			KrpcAntwort::FindNode { knoten_v4, knoten_v6 } => {
				if let Some(v) = knoten_v4 {
					en.emit_pair_with(b"nodes", |e| {
						e.emit_bytes(knotenliste_kodieren(v.iter()).as_slice())
					})?;
				}
				if let Some(v) = knoten_v6 {
					en.emit_pair_with(b"nodes6", |e| {
						e.emit_bytes(knotenliste_kodieren(v.iter()).as_slice())
					})?;
				}
			},
			KrpcAntwort::GetPeers { peers, knoten_v4, knoten_v6, token } => {
				if let Some(p) = peers {
					en.emit_pair_with(b"values", |e| peerliste_kodieren(e, p.iter()))?;
				}
				if let Some(v) = knoten_v4 {
					en.emit_pair_with(b"nodes", |e| {
						e.emit_bytes(knotenliste_kodieren(v.iter()).as_slice())
					})?;
				}
				if let Some(v) = knoten_v6 {
					en.emit_pair_with(b"nodes6", |e| {
						e.emit_bytes(knotenliste_kodieren(v.iter()).as_slice())
					})?;
				}
				en.emit_pair_with(b"token", |e| e.emit_bytes(token.as_slice()))?;
			},
			KrpcAntwort::AnnouncePeer => {},
			KrpcAntwort::SampleInfohashes { interval_sek, knoten_v4, knoten_v6, anz_infohashes, info_hashes } => {
				en.emit_pair(b"interval", interval_sek)?;
				if let Some(v) = knoten_v4 {
					en.emit_pair_with(b"nodes", |e| {
						e.emit_bytes(knotenliste_kodieren(v.iter()).as_slice())
					})?;
				}
				if let Some(v) = knoten_v6 {
					en.emit_pair_with(b"nodes6", |e| {
						e.emit_bytes(knotenliste_kodieren(v.iter()).as_slice())
					})?;
				}
				en.emit_pair(b"num",anz_infohashes)?;
				en.emit_pair_with(b"samples", |e| {
					e.emit_bytes(hashliste_kodieren(info_hashes.iter()).as_slice())
				})?;
			}
		}
		
		Ok(())
	}
	
	fn ping_argumente_einlesen<A: Addr>(obj: Object, ext_ip: Option<A>) -> Result<KrpcInhalt<A>, DeErr> {
		let mut d = obj.try_into_dictionary()?;
		dict_einlesen!(&mut d, b"id" => id: U160::decode_bencode_object);
		Ok(KrpcInhalt::Antwort {
			id: id.ok_or(DeErr::missing_field("id"))?,
			ext_ip,
			aw: KrpcAntwort::Ping
		})
	}
	
	fn find_node_argumente_einlesen<A: Addr>(obj: Object, ext_ip: Option<A>) -> Result<KrpcInhalt<A>, DeErr> {
		let mut d = obj.try_into_dictionary()?;
		dict_einlesen!(&mut d,
			b"id" => id: U160::decode_bencode_object,
			b"nodes" => knoten_v4: knotenliste_dekodieren,
			b"nodes6" => knoten_v6: knotenliste_dekodieren
		);
		Ok(KrpcInhalt::Antwort {
			id: id.ok_or(DeErr::missing_field("id"))?,
			ext_ip,
			aw: KrpcAntwort::FindNode {knoten_v4, knoten_v6}
		})
	}
	
	fn get_peers_argumente_einlesen<A: Addr>(obj: Object, ext_ip: Option<A>) -> Result<KrpcInhalt<A>, DeErr> {
		let mut d = obj.try_into_dictionary()?;
		dict_einlesen!(&mut d,
			b"id" => id: U160::decode_bencode_object,
			b"peers" => peers: peerliste_dekodieren,
			b"nodes" => knoten_v4: knotenliste_dekodieren,
			b"nodes6" => knoten_v6: knotenliste_dekodieren,
			b"token" => token: |w: Object| w.try_into_bytes().map(|v| v.to_vec())
		);
		Ok(KrpcInhalt::Antwort {
			id: id.ok_or(DeErr::missing_field("id"))?,
			ext_ip,
			aw: KrpcAntwort::GetPeers { peers, knoten_v4, knoten_v6, token:  token.ok_or(DeErr::missing_field("token"))?}
		})
	}
	
	fn announce_peer_argumente_einlesen<A: Addr>(obj: Object, ext_ip: Option<A>) -> Result<KrpcInhalt<A>, DeErr> {
		let mut d = obj.try_into_dictionary()?;
		dict_einlesen!(&mut d, b"id" => id: U160::decode_bencode_object);
		Ok(KrpcInhalt::Antwort {
			id: id.ok_or(DeErr::missing_field("id"))?,
			ext_ip,
			aw: KrpcAntwort::AnnouncePeer,
		})
	}
	
	fn sample_infohashes_argumente_einlesen<A: Addr>(obj: Object, ext_ip: Option<A>) -> Result<KrpcInhalt<A>, DeErr> {
		let mut d = obj.try_into_dictionary()?;
		dict_einlesen!(&mut d,
			b"id" => id: U160::decode_bencode_object,
			b"nodes" => knoten_v4: knotenliste_dekodieren,
			b"nodes6" => knoten_v6: knotenliste_dekodieren,
			b"interval" => interval_sek: |w: Object| w.try_into_integer()?.parse().map_err(DeErr::malformed_content),
			b"num" => anz_infohashes: |w: Object| w.try_into_integer()?.parse().map_err(DeErr::malformed_content),
			b"samples" => info_hashes: hashliste_dekodieren
		);
		Ok(KrpcInhalt::Antwort {
			id: id.ok_or(DeErr::missing_field("id"))?,
			ext_ip,
			aw: KrpcAntwort::SampleInfohashes {
				interval_sek: interval_sek.ok_or(DeErr::missing_field("interval"))?,
				knoten_v4, knoten_v6,
				anz_infohashes: anz_infohashes.ok_or(DeErr::missing_field("num"))?,
				info_hashes: info_hashes.ok_or(DeErr::missing_field("samples"))?,
			},
		})
	}
}

pub struct KrpcFehler {
	pub fehlercode: KrpcFehlercode,
	pub fehlermeldung: String,
}
impl ToBencode for KrpcFehler {
	const MAX_DEPTH: usize = 1;
	
	fn encode(&self, encoder: SingleItemEncoder) -> Result<(), EnErr> {
		encoder.emit_list(|e| {
			e.emit(&self.fehlercode)?;
			e.emit_str(&self.fehlermeldung)
		})
	}
}
impl FromBencode for KrpcFehler {
	const EXPECTED_RECURSION_DEPTH: usize = 1;
	
	fn decode_bencode_object(obj: Object) -> Result<Self, DeErr>
		where Self: Sized
	{
		match obj {
			Object::List(mut liste) => {
				let länge_err =|| DeErr::unexpected_token("Liste mit Länge == 2", "Liste mit Länge != 2").context("KrpcFehler");
				
				let fehlercode_obj = liste.next_object()?.ok_or_else(länge_err)?;
				let fehlercode = KrpcFehlercode::decode_bencode_object(fehlercode_obj)?;
				let fehlermeldung_obj = liste.next_object()?.ok_or_else(länge_err)?;
				let fehlermeldung = String::from_utf8_lossy(fehlermeldung_obj.try_into_bytes()?).into_owned();
				if liste.next_object()?.is_some() { return Err(länge_err()); }
				
				Ok(KrpcFehler { fehlercode, fehlermeldung})
			}
			_ => Err(DeErr::unexpected_token("Liste mit Länge == 2", "keine Liste").context("KrpcFehler")),
		}
	}
}


// TODO dreifache Doplikation der Bedeutung der Fehlercodes vermeiden.
#[derive(PartialEq)]
#[repr(u16)]
pub enum KrpcFehlercode {
	Allgemein = 201,
	ServerFehler = 202,
	ProtokollFehler = 203,
	UnbekannteMethode = 204,
	Unbekannt(u64),
}
impl From<u64> for KrpcFehlercode {
	fn from(int: u64) -> Self {
		match int {
			201 => KrpcFehlercode::Allgemein,
			202 => KrpcFehlercode::ServerFehler,
			203 => KrpcFehlercode::ProtokollFehler,
			204 => KrpcFehlercode::UnbekannteMethode,
			sonstiges => KrpcFehlercode::Unbekannt(sonstiges),
		}
	}
}
impl FromBencode for KrpcFehlercode {
	const EXPECTED_RECURSION_DEPTH: usize = 0;
	
	fn decode_bencode_object(obj: Object) -> Result<Self, DeErr>
		where Self: Sized
	{
		obj.try_into_integer()?
			.parse::<u64>()
			.map(KrpcFehlercode::from)
			.map_err(|e| DeErr::malformed_content(e).context("KrpcFehlercode"))
	}
}

impl ToBencode for KrpcFehlercode {
	const MAX_DEPTH : usize = 0;
	
	fn encode(&self, encoder: SingleItemEncoder) -> Result<(), EnErr> {
		match self {
			KrpcFehlercode::Allgemein => encoder.emit_int(201),
			KrpcFehlercode::ServerFehler => encoder.emit_int(202),
			KrpcFehlercode::ProtokollFehler => encoder.emit_int(203),
			KrpcFehlercode::UnbekannteMethode => encoder.emit_int(204),
			KrpcFehlercode::Unbekannt(n) => encoder.emit_int(*n),
		}
	}
}

impl FromBencode for U160 {
	const EXPECTED_RECURSION_DEPTH: usize = 0;
	
	fn decode_bencode_object(obj: Object) -> Result<Self, DeErr>
		where Self: Sized
	{
		Ok(U160(obj.try_into_bytes()?.try_into().map_err(|e| DeErr::malformed_content(e).context("U160"))?))
	}
}
impl ToBencode for U160 {
	const MAX_DEPTH: usize = 0;
	
	fn encode(&self, encoder: SingleItemEncoder) -> Result<(), EnErr> {
		encoder.emit_bytes(self.0.as_slice())
	}
}

fn peerliste_kodieren<'a, I: Iterator<Item=&'a SocketAddr>>(en: SingleItemEncoder, iter: I) -> Result<(), EnErr> {
	let mut v = Vec::with_capacity(SocketAddrV4::KRPC_LEN.max(SocketAddrV6::KRPC_LEN));
	en.emit_list(|e| {
		for addr in iter {
			match addr {
				SocketAddr::V4(a) => {
					v.resize(SocketAddrV4::KRPC_LEN, 0);
					a.als_krpc_bytes(v.as_mut_slice());
				},
				SocketAddr::V6(a) => {
					v.resize(SocketAddrV6::KRPC_LEN, 0);
					a.als_krpc_bytes(v.as_mut_slice());
				},
			};
			e.emit_bytes(v.as_slice())?;
		}
		Ok(())
	})
}

// Wir können `<Vec<SocketAddr>> as FromBencode` nicht benuzten, da wir
// wegen den Trait-Regeln `FromBencode` nicht für `SocketAddr` implementieren
// können.
fn peerliste_dekodieren(obj: Object) -> Result<Vec<SocketAddr>, DeErr> {
	let mut liste_obj = obj.try_into_list()?;
	let mut liste_erg = Vec::new();
	while let Some(obj2) = liste_obj.next_object()? {
		let peer_bytes = obj2.try_into_bytes()?;
		match peer_bytes.len() {
			6 => liste_erg.push(SocketAddrV4::aus_krpc_bytes(peer_bytes).unwrap().into()),
			18 => liste_erg.push(SocketAddrV6::aus_krpc_bytes(peer_bytes).unwrap().into()),
			_ => return Err(DeErr::unexpected_token("Bytestring mit Länge 6 doer 18 („Compact node info“)", format!("Bytestring mit Länge == {}", peer_bytes.len()))),
		}
	}
	
	Ok(liste_erg)
}

fn hashliste_kodieren<'a, I: Iterator<Item=&'a U160>>(iter: I) -> Vec<u8> {
	let mut vec = Vec::with_capacity(iter.size_hint().0);
	for i in iter {
		vec.extend_from_slice(i.0.as_slice());
	}
	
	vec
}

fn hashliste_dekodieren(obj: Object) -> Result<Vec<U160>, DeErr> {
	let bytes = obj.try_into_bytes()?;
	if bytes.len() % 20 != 0 { return Err(DeErr::unexpected_token("bytes mit Länge % 20 == 0", "bytes mit Länge % 20 != 0")); }
	// unwrap: Err hängt nur von der Länge der slice ab und diese ist durch
	// `slice::chunks` und die obrige if-Anweisung garantiert.
	Ok(bytes.chunks(20).map(|s| U160(s.try_into().unwrap())).collect())
}


fn knotenliste_kodieren<'a, A: Addr + 'a, I: Iterator<Item=&'a KnotenInfo<A>>>(iter: I) -> Vec<u8> {
	let mut vec = Vec::with_capacity(iter.size_hint().0);
	for k in iter {
		vec.extend_from_slice(k.id.0.as_slice());
		let addr_anfang = vec.len();
		vec.resize(addr_anfang+A::KRPC_LEN,0);
		k.addr.als_krpc_bytes(&mut vec[addr_anfang..]);
	}
	
	vec
}

fn knotenliste_dekodieren<A: Addr>(obj: Object) -> Result<Vec<KnotenInfo<A>>, DeErr> {
	let bytes = obj.try_into_bytes()?;
	if bytes.len() % (A::KRPC_LEN + 20) != 0 {
		return Err(DeErr::unexpected_token("Bytestring mit Länge % (A::KRPC_LEN + 20) == 0", format!("Bytestring mit Länge == {}",bytes.len())));
	}
	Ok(
		bytes.chunks(A::KRPC_LEN + 20).map(|b| {
			let id = U160(b[0..20].try_into().unwrap());
			let addr = A::aus_krpc_bytes(&b[20..]).unwrap();
			KnotenInfo {id, addr}
		}).collect()
	)
}

fn implizieten_port_dekodieren(obj: Object) -> Result<bool, DeErr> {
	Ok(obj.try_into_integer()?
		.parse::<u8>()
		.map_err(|e| DeErr::malformed_content(e))?
		== 1)
}

#[derive(PartialEq, Copy, Clone)]
pub enum Will {
	Ipv6,
	Ipv4,
	Beide,
}
impl Will {
	pub fn v4(&self) -> bool {
		match self {
			Will::Ipv6 => false,
			Will::Ipv4 => true,
			Will::Beide => true,
		}
	}
	pub fn v6(&self) -> bool {
		match self {
			Will::Ipv6 => true,
			Will::Ipv4 => false,
			Will::Beide => true,
		}
	}
}
impl FromBencode for Will {
	const EXPECTED_RECURSION_DEPTH: usize = 1;
	
	fn decode_bencode_object(obj: Object) -> Result<Self, DeErr>
		where Self: Sized
	{
		let mut liste = obj.try_into_list()?;
		let mut ipv4 = false;
		let mut ipv6 = false;
		
		while let Some(elem) = liste.next_object()? {
			match elem.try_into_bytes()? {
				b"n4" => ipv4 = true,
				b"n6" => ipv6 = true,
				_ => {},
			}
		}
		
		match (ipv4, ipv6) {
			(true, true) => Ok(Will::Beide),
			(true, false) => Ok(Will::Ipv4),
			(false, true) => Ok(Will::Ipv6),
			(false, false) => Err(DeErr::unexpected_token("Want-Liste mit „n4“, „n6“ oder beidem", "Want-Liste mit weder „n4“ noch „n6“")),
		}
	}
}
impl ToBencode for Will {
	const MAX_DEPTH : usize = 1;
	
	fn encode(&self, encoder: SingleItemEncoder) -> Result<(), EnErr> {
		encoder.emit_list(|e| {
			if *self == Will::Ipv4 || *self == Will::Beide {e.emit_str("n4")?;}
			if *self == Will::Ipv6 || *self == Will::Beide {e.emit_str("n6")?;}
			Ok(())
		})
	}
}
