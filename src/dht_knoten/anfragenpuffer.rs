use bitvec::prelude::*;
use super::*;
use event_listener::{Event, EventListener};
use oneshot::Sender as EinzelSender;

#[derive(Debug)]
pub enum Anfrageergebnis {
	Ok(KrpcAntwort),
	Fehler(KrpcFehler),
	Zeitüberschreitung,
	UngültigeAntwort,
}

pub struct AusstehendeAnfrage {
	pub zeitgrenze: Instant,
	pub methode: AnfrageMethode,
	pub aufgabenbereich: &'static str,
	pub bei_fehler_knoten_entfernen: bool,
	pub knoten_id: U160,
	pub sender: EinzelSender<Anfrageergebnis>,
}

pub struct Anfragenpuffer {
	index: BitBox,
	anfragen: Vec<Option<AusstehendeAnfrage>>,
	anfrage_frei: Event,
}
impl Anfragenpuffer {
	pub fn neu(max_anfragen: usize) -> Self {
		let mut p = Anfragenpuffer {
			index: bitbox![0;max_anfragen],
			anfragen: Vec::with_capacity(max_anfragen),
			anfrage_frei: Event::new(),
		};
		for _ in 0..max_anfragen {
			p.anfragen.push(None);
		}
		p
	}
	
	/// Entfernt die Anfrage mit der Nummer `idx` und gibt diese zurück falls sie existiert.
	pub fn nehmen(&mut self, idx: usize) -> Option<AusstehendeAnfrage> {
		if idx > self.index.len() { return None; }
		self.index.set(idx, false);
		
		let anf_opt = self.anfragen[idx].take();
		if anf_opt.is_some() {
			self.anfrage_frei.notify(1);
		}
		
		anf_opt
	}
	
	pub fn nehmen_bytes(&mut self, txid: &[u8]) -> Option<AusstehendeAnfrage> {
		if txid.len() != 2 {
			log::debug!("Antwort mit ungültiger Transaktionsnummer: {:02X?}", txid);
			return None;
		}
		let idx = u16::from_be_bytes(txid.try_into().unwrap()) as usize;
		self.nehmen(idx)
	}
	
	pub fn methode_für_txid(&self, idx: usize) -> Option<AnfrageMethode> {
		self.anfragen.get(idx).map(|opt| opt.as_ref()).flatten().map(|anf| anf.methode.clone())
	}
	
	/// Fügt eine Neue Anfrage in den Puffer ein und gibt die zugewiesene ID zurück.
	/// Solle die maximale Anzahl an einträgen im Puffer erreicht sein wird die Anfrage
	/// und ein `EventListener` als `Err(_)` zurückgegeben. Wenn der `EventListener`
	/// Ein Ereignis empfängt sollte das Einfügen der Anfrage erneut versucht werden.
	pub fn einfügen(&mut self, anfrage: AusstehendeAnfrage) -> Result<usize, (AusstehendeAnfrage, EventListener)> {
		if let Some(idx) = self.index.first_zero() {
			self.index.set(idx, true);
			assert!(self.anfragen[idx].replace(anfrage).is_none());
			Ok(idx)
		} else {
			Err((anfrage, self.anfrage_frei.listen()))
		}
	}
	
	/// Gibt einen Iterator zurück, der für jede ausstehende Anfrage überprüft
	/// ob die Zeitgrenze abgelaufen ist und die KnotenIds der Zielknoten der
	/// abgelaufenen Anfragen aus.
	/// 
	/// Nachdem der Iterator durchlaufen wurde, kann er die baldigste Zeitgrende
	/// der noch nicht abgelaufenen Anfragen ausgeben.
	/// 
	pub fn zeitgrenzen_überprüfen<'a>(&'a mut self) -> ZeitüberschreitungsIter<'a> {
		ZeitüberschreitungsIter {
			anfragen_iter: self.anfragen.iter_mut().enumerate(),
			index: &mut self.index,
			anfrage_frei: &self.anfrage_frei,
			baldigste_zeitgrenze: None,
		}
	}
}

pub struct ZeitüberschreitungsIter<'a> {
	anfragen_iter: std::iter::Enumerate<std::slice::IterMut<'a, Option<AusstehendeAnfrage>>>,
	index: &'a mut BitBox,
	anfrage_frei: &'a Event,
	baldigste_zeitgrenze: Option<Instant>,
}
impl<'a> ZeitüberschreitungsIter<'a> {
	/// Gibt die baldigste (nicht abgelaufene) Zeitgrenze zurück. Oder `None`
	/// falls es keine gab.
	/// 
	/// Panict wenn der Iterator nicht vollständig durchlaufen wurde.
	/// 
	/// Hinweis: Da zwischen dem Zeitpunkt der Überprüfung und dem return dieser
	/// Funktion Zeit vergangen sein kann ist es nicht garrantiert das der
	/// zurückgebebene Zeitpunkt in der Zukunft liegt.
	pub fn baldigste_zeitgrenze(mut self) -> Option<Instant> {
		while self.next().is_some() {}
		self.baldigste_zeitgrenze
	}
}
impl<'a> Iterator for ZeitüberschreitungsIter<'a> {
	type Item = U160;
	
	fn next(&mut self) -> Option<Self::Item> {
		while let Some((idx, eintrag)) = self.anfragen_iter.next() {
			if let Some(anfrage_ref) = eintrag.as_ref() {
				if anfrage_ref.zeitgrenze < Instant::now() {
					let anfrage = eintrag.take().unwrap();
					self.index.set(idx,false);
					#[allow(unused_must_use)]
					anfrage.sender.send(Anfrageergebnis::Zeitüberschreitung);
					self.anfrage_frei.notify(1);
					return Some(anfrage.knoten_id);
				} else {
					if let Some(ref mut z) = self.baldigste_zeitgrenze {
						if *z > anfrage_ref.zeitgrenze {
							*z = anfrage_ref.zeitgrenze;
						}
					} else {
						self.baldigste_zeitgrenze = Some(anfrage_ref.zeitgrenze);
					}
				}
			}
		}
		None
	}
	
	fn size_hint(&self) -> (usize, Option<usize>) {
		self.anfragen_iter.size_hint()
	}
}

