use std::collections::{BTreeMap, BinaryHeap};
use std::net::{SocketAddrV4, SocketAddrV6};

use crate::datentypen::U160;
use crate::addr_generisch::*;
use super::*;

pub struct Suchvorgang<A: Addr> {
	knoten: Arc<DhtKnoten<A>>,

}

#[derive(Copy, Clone, PartialEq, Debug)]
pub enum Suchzielart {
	NurKnoten,
	Peers,
	Infohashes,
}

impl<A: Addr> DhtKnoten<A> {
	pub async fn knotensuche<FP, FI, FK>(
		self: Arc<Self>,
		ziel: U160,
		zielart: Suchzielart,
		priorität: Prio,
		bei_peer: FP,
		bei_infohash: FI,
		bei_knoten: FK,
	) -> Vec<KnotenInfo<A>>
	where
		FP: FnMut(SocketAddr),
		FI: FnMut(U160),
		FK: FnMut(&Vec<KnotenInfo<A>>),
	{
		let mut bisher_angefragt = Vec::with_capacity(K);
		
		let max_kandidaten = K;
		let ges_zeitgrenze = self.anfragen_zeitgrenze * 5;
		let startzeitpunkt = Instant::now();
		
		
		let mut entf_nächstgelegender: U160;
		let mut ausstehende_anfragen = Vec::new();
		let mut kandidaten: BTreeMap<U160, KnotenInfo<A>> = BTreeMap::new();
		
		let (tx, rx) = smol::channel::bounded(K);
		
		
		let anfangskandidaten = self
			.routing_tabelle
			.read()
			.unwrap()
			.nächste_k_knoten(ziel)
			.into_iter()
			.map(|k| KnotenInfo { id: k.0, addr: *k.1 })
		
		// TODO
		for gegenstelle in anfangskandidaten {
			let tx2 = tx.clone();
			let fut = async {
				let res =  self.knotensuchschritt(gegenstelle, ziel, zielart, priorität, bei_peer, bei_infohash).await;
				if let Some(infos) = res {
					if !infos.is_empty() {
						let _ = tx2.send(infos).await;
					}
				}
			}
		}
		
		kandidaten_einfügen(&mut kandidaten, anfangskandidaten, ziel);
		
		
		
		loop {
			// TODO
			
		}
		
	}
	
	/// Fragt bei der gegebenen Gegenstelle nach dem Ziel und gibt die
	/// nahegelegensten Knoten zurück die die angefragte Gegenstelle kennt.
	/// 
	/// Je nach Zielart wird bei zurückgegebenen Infohashes bzw. gefunden Peers
	/// `bei_peer` bzw. `bei_infohash` aufgerufen. Diese Funktionen dürfen nicht
	/// blocken.
	/// 
	/// Hinweis: Wenn Zielart Peers ist und die Gegenstelle Peers kennt gibt die
	/// gegenstelle keine Knoten zurück. In diesem Fall gibt diese Funktion
	/// einen leeren Vec zurück.
	/// 
	/// Bei Fehlern wird `None` zurückgegeben.
	async fn knotensuchschritt<FP, FI>(
		self: Arc<Self>,
		gegenstelle: KnotenInfo<A>,
		ziel: U160,
		zielart: Suchzielart,
		priorität: Prio,
		bei_peer: FP,
		bei_infohash: FI,
	) -> Option<Vec<KnotenInfo<A>>>
	where
		FP: FnMut(SocketAddr),
		FI: FnMut(U160),
	{
		use Suchzielart::*;
		let anf = match zielart {
			NurKnoten => KrpcAnfrage::FindNode { ziel, will: None },
			Infohashes => KrpcAnfrage::SampleInfohashes { ziel, will: None },
			Peers => KrpcAnfrage::GetPeers { info_hash: ziel, will: None }
		};
		// TODO Self::anfrage_senden sollte bei_fehler_knoten_entfernen selbst bestimmen.
		let bei_fehler_knoten_entfernen = zielart==Infohashes;
		let aw = self.anfrage_senden(gegenstelle, anf, priorität, bei_fehler_knoten_entfernen).await.ok()?;
		let erg = aw.await.ok()?;
		
		if let Anfrageergebnis::Ok(krpc_aw) = erg {
			// asserts: Die Entsprechung der Antwort auf die Anfrage wird bei der
			// deserialisierung sichergestellt; Wenn die Gegenstelle eine nicht
			// passende Antwort sendet verursacht das einen Deserialisierungsfehler.
			let (knoten_v4, knoten_v6) = match krpc_aw {
				KrpcAntwort::FindNode { knoten_v4, knoten_v6 } => {
					assert_eq!(zielart, NurKnoten);
					(knoten_v4, knoten_v6)
				},
				KrpcAntwort::GetPeers { peers, knoten_v4, knoten_v6, token :_ } => {
					assert_eq!(zielart, Peers);
					peers.unwrap_or_default().into_iter().for_each(bei_peer);
					(knoten_v4, knoten_v6)
				},
				KrpcAntwort::SampleInfohashes { interval_sek: _, knoten_v4, knoten_v6, anz_infohashes: _, info_hashes } => {
					assert_eq!(zielart, Infohashes);
					info_hashes.into_iter().for_each(bei_infohash);
					(knoten_v4, knoten_v6)
				},
				_ => unreachable!(),
			};
			if A::IST_IPV4 {
				Some(knoten_v4.unwrap_or_default().into_iter().map(knoten_info_murks4).collect())
			} else {
				Some(knoten_v6.unwrap_or_default().into_iter().map(knoten_info_murks6).collect())
			}
		} else {
			None
		}
	}
}



fn kandidaten_einfügen<A: Addr,I: Iterator<Item=KnotenInfo<A>>>(
	map: &mut BTreeMap<U160, KnotenInfo<A>>,
	i: I,
	ziel: U160,
) {
	for k in i {
		map.insert(ziel ^ k.id, k);
	}
}

// TODO Das wir solche Murksfunktionen brauchen bedeutet das unser Umgang mit
//      der IP-Versionsdichotome Murks ist -.-

fn knoten_info_murks4<A: Addr>(info: KnotenInfo<SocketAddrV4>) -> KnotenInfo<A> {
	KnotenInfo { id: info.id, addr: A::aus_socket_addr(SocketAddr::V4(info.addr)).unwrap()}
}

fn knoten_info_murks6<A: Addr>(info: KnotenInfo<SocketAddrV6>) -> KnotenInfo<A> {
	KnotenInfo { id: info.id, addr: A::aus_socket_addr(SocketAddr::V6(info.addr)).unwrap()}
}

