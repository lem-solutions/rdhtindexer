use metrics::*;
use std::num::Saturating;
use std::time::{Duration, Instant};

use crate::datentypen::*;

/// Maxiamle Anzahl Knoten in einem „Bucket“
pub(crate) const K: usize = 8;

/// Maximale Anzahl an nicht beantworteten Anfragen bevor wir einen Knoten als „schelcht“ klassifizieren.
const MAX_FEHLER: Saturating<u16> = Saturating(1);

/// Zeit ohne Nachricht nach der ein Knoten als „Fragwürdig“ gilt.
const ZEITFENSTER: Duration = Duration::from_secs(15 * 60);

pub(crate) const ROUTING_TABELLE_ZEITFENSTER: Duration = ZEITFENSTER;

#[derive(PartialEq, Eq)]
enum KnotenStatus {
	Gut,
	Fragwürdig,
	Schlecht,
}

#[derive(Clone)]
pub struct KnotenInfo<T> {
	/// Hat dieser Knoten jemals auf eine Anfrage von uns geantwortet?
	hat_geantwortet: bool,
	/// Zeitpunkt der letzen Anfrage oder Antwort die wir von diesem Knoten bekommen haben
	letzte_nachicht: Option<Instant>,
	/// Anzahl der unbeantworteten Anfragen seit der letzten erfolgreichen.
	fehleranzahl: Saturating<u16>,
	pub addr: T,
	pub id: U160,
}
impl<T> KnotenInfo<T> {
	fn status(&self) -> KnotenStatus {
		if self.fehleranzahl > MAX_FEHLER {
			return KnotenStatus::Schlecht;
		}
		if self.hat_geantwortet
			&& self
				.letzte_nachicht
				.map(|i| i.elapsed() <= ZEITFENSTER)
				.unwrap_or(false)
			&& self.fehleranzahl.0 == 0
		{
			return KnotenStatus::Gut;
		}
		KnotenStatus::Fragwürdig
	}
}

struct Bucket<T> {
	knoten: [Option<KnotenInfo<T>>; K],
}
impl<T> Default for Bucket<T> {
	fn default() -> Self {
		Bucket {
			knoten: [const { None }; K],
		}
	}
}

impl<T> Bucket<T> {
	fn knoten_mit_id_mut<F: Fn(&mut KnotenInfo<T>)>(
		&mut self,
		id: U160,
		f: &F,
	) -> bool {
		for knotenslot in self.knoten.iter_mut() {
			if knotenslot.is_none() {
				continue;
			}
			let knoten = knotenslot.as_mut().unwrap();
			if knoten.id == id {
				f(knoten);
				return true;
			}
		}
		false
	}

	// Der Knoten darf nicht bereits existieren, das wäre ein Logikfehler.
	// Some(T) bedeutet konnte nicht eingefügt werden und addr wird zu weiterverwendung zurückgegeben.
	fn knoten_einfügen<F: Fn(&mut KnotenInfo<T>)>(
		&mut self,
		id: U160,
		addr: T,
		fragwürdig_ersetzen: bool,
		f: &F,
	) -> Option<T> {
		for slot in self.knoten.iter_mut() {
			if slot
				.as_ref()
				.map(|k| k.status() == KnotenStatus::Schlecht)
				.unwrap_or(true)
			{
				*slot = Some(KnotenInfo {
					hat_geantwortet: false,
					letzte_nachicht: None,
					fehleranzahl: Saturating(0),
					addr: addr,
					id: id,
				});
				f(slot.as_mut().unwrap());
				return None;
			}
		}

		if !fragwürdig_ersetzen {
			return Some(addr);
		}

		// Hier müssen alle Slots belegt sein, deswegen geht slot.unwrap()
		for slot in self.knoten.iter_mut() {
			if slot.as_ref().unwrap().status() == KnotenStatus::Fragwürdig {
				*slot = Some(KnotenInfo {
					hat_geantwortet: false,
					letzte_nachicht: None,
					fehleranzahl: Saturating(0),
					addr: addr,
					id: id,
				});
				f(slot.as_mut().unwrap());
				return None;
			}
		}

		Some(addr)
	}
}

pub struct RoutingTabelle<T> {
	buckets: Vec<Bucket<T>>,
	pub eigene_id: U160,
}
impl<T> RoutingTabelle<T> {
	pub fn neu() -> Self {
		RoutingTabelle {
			buckets: vec![Bucket::default()],
			eigene_id: U160::zufällig(),
		}
	}

	pub fn iter_fragwürdige_knoten(
		&self,
	) -> impl Iterator<Item = &KnotenInfo<T>> {
		self
			.buckets
			.iter()
			.map(|b| b.knoten.iter())
			.flatten()
			.filter_map(|k| k.as_ref())
			.filter(|k| k.status() == KnotenStatus::Fragwürdig)
	}

	pub fn fragwürdigen_knoten_finden(&self) -> Option<&KnotenInfo<T>> {
		self.iter_fragwürdige_knoten().next()
	}

	pub fn iter_gute_knoten(&self) -> impl Iterator<Item = &KnotenInfo<T>> {
		self
			.buckets
			.iter()
			.map(|b| b.knoten.iter())
			.flatten()
			.filter_map(|k| k.as_ref())
			.filter(|k| k.status() == KnotenStatus::Gut)
	}

	pub fn knoten_extrahieren(self) -> impl Iterator<Item = KnotenInfo<T>> {
		self
			.buckets
			.into_iter()
			.map(|b| b.knoten.into_iter())
			.flatten()
			.filter_map(|k| k)
			.filter(|k| {
				k.status() == KnotenStatus::Gut
					|| k.status() == KnotenStatus::Fragwürdig
			})
	}

	pub fn nächster_knoten(&self, ziel: U160) -> Option<(U160, &T)> {
		let mut iter = self.iter_gute_knoten();
		let mut bester = if let Some(k) = iter.next() {
			(k.id, &k.addr)
		} else {
			return None;
		};

		for knoten in iter {
			if knoten.id ^ ziel < bester.0 ^ ziel {
				bester = (knoten.id, &knoten.addr);
			}
		}

		Some(bester)
	}

	pub fn nächste_k_knoten<'a>(
		&'a self,
		ziel: U160,
	) -> noalloc_vec_rs::vec::Vec<(U160, &'a T), K> {
		let mut puffer = noalloc_vec_rs::vec::Vec::new();

		for knoten in self.iter_gute_knoten() {
			let mut idx = 0;
			for (i, (k_id, _k_addr)) in puffer.iter().enumerate() {
				idx = i;
				if *k_id ^ ziel < knoten.id ^ ziel {
					break;
				}
			}
			if idx < K {
				if puffer.len() == K {
					#[allow(unused_must_use)]
					puffer.pop();
				}
				puffer.insert(idx, (knoten.id, &knoten.addr)).unwrap();
			}
		}

		puffer
	}

	fn bucket_für_id(&self, id: U160) -> &Bucket<T> {
		assert!(self.buckets.len() > 0);
		&self.buckets[self
			.eigene_id
			.erstes_ungleiches_bit(id)
			.min(self.buckets.len() - 1)]
	}

	fn bucket_für_id_mut(&mut self, id: U160) -> &mut Bucket<T> {
		let len = self.buckets.len();
		assert!(len > 0);
		&mut self.buckets[self.eigene_id.erstes_ungleiches_bit(id).min(len - 1)]
	}

	fn bucket_aufteilen(&mut self) {
		let len = self.buckets.len();
		assert!(K >= 8);
		// Theoretisch könnte es bis zu 160 Buckets geben, aber der letzte Bucket könnte höchstens einen Knoten beinhalten.
		// deswegen ist die Anzahl der Buckets logisch auf weniger als 160 begrenzt.
		assert!(len < 157);

		let mut neuer_bucket = Bucket::default();
		let bestehender_bucket = self.buckets.last_mut().unwrap();
		for knoten_slot in bestehender_bucket.knoten.iter_mut() {
			let mut verschieben = false;
			if let Some(knoten) = knoten_slot {
				let eub = knoten.id.erstes_ungleiches_bit(self.eigene_id);
				assert!(eub >= len - 1);
				if eub > len - 1 {
					verschieben = true;
				}
			}

			if verschieben {
				// Da die Buckets gleich groß sind wird es immer einen freien Slot geben.
				*neuer_bucket
					.knoten
					.iter_mut()
					.find(|x| x.is_none())
					.unwrap() = knoten_slot.take()
			}
		}
		self.buckets.push(neuer_bucket);
	}

	// Wenn der Knoten gefunden oder eingefügt wurde, wird `f` mit einer Referenz auf diesen Knoten ausgeführt und `true` zurückgegeben,
	// andernfalls wird `false` zurückgegeben
	fn knoten_finden_oder_einfügen<F: Fn(&mut KnotenInfo<T>)>(
		&mut self,
		id: U160,
		addr: T,
		fragwürdig_ersetzen: bool,
		f: &F,
	) -> bool {
		let bucket = self.bucket_für_id_mut(id);
		if bucket.knoten_mit_id_mut(id, f) {
			return true;
		}
		let mut addr = if let Some(a) =
			bucket.knoten_einfügen(id, addr, fragwürdig_ersetzen, f)
		{
			a
		} else {
			self.metriken();
			return true;
		};

		let max_bucket = self.eigene_id.erstes_ungleiches_bit(id);
		while max_bucket >= self.buckets.len() {
			self.bucket_aufteilen();
			addr = if let Some(a) = self.buckets.last_mut().unwrap().knoten_einfügen(
				id,
				addr,
				fragwürdig_ersetzen,
				f,
			) {
				a
			} else {
				self.metriken();
				return true;
			};
		}
		false
	}

	pub fn anfrage_erhalten(&mut self, id: U160, addr: T) {
		self.knoten_finden_oder_einfügen(id, addr, false, &|k| {
			k.letzte_nachicht = Some(Instant::now())
		});
	}

	pub fn antwort_erhalten(&mut self, id: U160, addr: T) {
		self.knoten_finden_oder_einfügen(id, addr, true, &|k| {
			k.letzte_nachicht = Some(Instant::now());
			k.hat_geantwortet = true;
			k.fehleranzahl = Saturating(0);
		});
	}

	pub fn fehlschlag(&mut self, id: U160) {
		self
			.bucket_für_id_mut(id)
			.knoten_mit_id_mut(id, &|k| k.fehleranzahl += 1);
	}

	pub fn knoten_info_einfügen(&mut self, info: KnotenInfo<T>) {
		let fragwürdig_ersetzen = info.status() == KnotenStatus::Gut;
		self.knoten_finden_oder_einfügen(
			info.id,
			info.addr,
			fragwürdig_ersetzen,
			&|k| {
				k.letzte_nachicht = info.letzte_nachicht;
				k.hat_geantwortet = info.hat_geantwortet;
				k.fehleranzahl = info.fehleranzahl;
			},
		);
	}

	fn metriken(&self) {
		let anz_gut = self.iter_gute_knoten().count();
		let anz_fragwürdig = self.iter_fragwürdige_knoten().count();
		gauge!("routing_tabelle anz_gut").set(anz_gut as f64);
		gauge!("routing_tabelle anz_fragwürdig").set(anz_fragwürdig as f64);
	}
}
