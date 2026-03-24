use std::time::{Duration, Instant};
use std::collections::{HashMap, VecDeque, BTreeMap};
use rand::prelude::IteratorRandom;
use crate::datentypen::*;


// OPTIMIEREN Diese Datenstruktur ist nicht gut durchdacht und wahrscheinlich
//            überkompliziert, ist aber wahrscheinlich unkritisch.

/// Speichert Peers und löscht die ältesten Peers bzw. Torrents wenn die
/// maximale Anzahl erreicht wird.
pub struct PeerTabelle<T: PartialEq> {
	peers: HashMap<U160, VecDeque<(Instant, T)>>,
	info_hash_alter: BTreeMap<Instant, U160>,
	max_torrents: usize,
	max_peers_pro_torrent: usize,
	max_alter: Duration,
	bep51_interval: Duration,
	bep51_auswahl: Vec<U160>,
	bep51_auswahl_größe: usize,
	bep51_auswahl_zeitstempel: Instant,
}

enum EvtlIter<I: Iterator + ExactSizeIterator> {
	Iter(I),
	Leer,
}
impl<I: Iterator + ExactSizeIterator> Iterator for EvtlIter<I> {
	type Item = I::Item;
	
	fn next(&mut self) -> Option<Self::Item> {
		match self {
			Self::Iter(i) => i.next(),
			Self::Leer => None,
		}
	}
	
	fn size_hint(&self) -> (usize, Option<usize>) {
		match self {
			Self::Iter(i) => i.size_hint(),
			Self::Leer => (0, None),
		}
	}
}
impl<I: Iterator + ExactSizeIterator> ExactSizeIterator for EvtlIter<I> {
	fn len(&self) -> usize {
		match self {
			Self::Iter(i) => i.len(),
			Self::Leer => 0,
		}
	}
}

impl<T: PartialEq> PeerTabelle<T> {
	pub fn neu(
		max_torrents: usize,
		max_peers_pro_torrent: usize,
		max_alter: Duration,
		bep51_auswahl_größe: usize,
		bep51_interval: Duration,
	) -> Self {
		assert!(max_torrents > 0);
		assert!(max_peers_pro_torrent > 0);
		assert!(bep51_interval <= Duration::from_hours(6));
		PeerTabelle {
			peers: HashMap::new(),
			info_hash_alter: BTreeMap::new(),
			max_torrents,
			max_peers_pro_torrent,
			max_alter,
			bep51_interval,
			bep51_auswahl: Vec::with_capacity(bep51_auswahl_größe),
			bep51_auswahl_größe,
			bep51_auswahl_zeitstempel: Instant::now()
		}
	}
	
	/// Gibt einen Iterator über alle bekannten Peers für den gegebenen `hash` zurück.
	/// Die aktuellsten werden als erstes zurückgegeben.
	pub fn peers_für_hash(&mut self, hash: &U160) -> impl Iterator<Item=&T> + ExactSizeIterator {
		if let Some(v) = self.peers.get(hash) {
			EvtlIter::Iter(v.iter().rev().map(|(_, x)| x))
		} else {
			EvtlIter::Leer
		}
	}
	
	pub fn bep51_anfrage(&mut self) -> &Vec<U160> {
		if self.bep51_auswahl.len() < self.bep51_auswahl_größe || self.bep51_auswahl_zeitstempel.elapsed() > self.bep51_interval {
			self.alte_hashes_entfernen();
			self.bep51_auswahl = self.peers.keys().cloned().choose_multiple(&mut rand::rng(), self.bep51_auswahl_größe);
		}
		&self.bep51_auswahl
	}
	
	pub fn anz_torrents(&self) -> usize {
		self.peers.len()
	}
	
	pub fn peer_einfügen(&mut self, info_hash: U160, peer: T) {
		self.alte_hashes_entfernen();
		let mut hash_neu_eingefügt = false;
		let v = self.peers.entry(info_hash).or_insert_with(|| {
			hash_neu_eingefügt = true;
			VecDeque::with_capacity(self.max_peers_pro_torrent)
		});
		if !hash_neu_eingefügt {
			// entfernt den alten Alterseintrag
			self.info_hash_alter.extract_if(..,|_k, v| v == &info_hash).next().unwrap();
		}
		self.info_hash_alter.insert(Instant::now(), info_hash);
		
		assert!(v.len() <= self.max_peers_pro_torrent);
		// veraltete Peers entfernen
		while v.front().map(|(zeit, _)| zeit.elapsed() > self.max_alter).unwrap_or(false) {
			v.pop_front();
		}
		if let Some(idx) = v.iter().position(|&(_zeit, ref p)| p == &peer) {
			// Wenn ein alter Eintrag vorhanden ist, ihn entfernen.
			v.remove(idx);
		} else if v.len() == self.max_peers_pro_torrent {
			// Wenn die Peer liste immernoch voll ist den ältesten Peer entfernen.
			v.pop_front();
		}
		v.push_back((Instant::now(), peer));
		
		assert!(self.peers.len() <= self.max_torrents + 1);
		if self.peers.len() > self.max_torrents {
			self.peers.remove(&self.info_hash_alter.pop_first().unwrap().1);
			assert_eq!(self.info_hash_alter.len(), self.peers.len());
		}
	}
	
	fn alte_hashes_entfernen(&mut self) {
		while let Some(e) = self.info_hash_alter.first_entry() {
			if e.key().elapsed() <= self.max_alter { break; }
			self.peers.remove(e.get());
			e.remove();
		}
		assert_eq!(self.info_hash_alter.len(), self.peers.len());
	}
}
