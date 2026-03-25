use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

#[derive(Copy, Clone)]
pub enum Prio {
	Antworten,
	Metadaten,
	Scannen,
}
impl Prio {
	// Fikive Bytes, mehr bedeute nidrigere Priorität.
	fn prio_bytes(&self) -> isize {
		match self {
			Prio::Scannen => 4000,
			Prio::Metadaten => 2000,
			Prio::Antworten => 0,
		}
	}
}

// Diese Implementierung ist nicht Überlaufssicher; Theoretisch können die isize
// Variablen überlaufen.

enum TempoErg {
	/// Die Bytes wurden entnommen und der Aufrufer muss nicht warten.
	Fertig,
	/// Die Bytes wurden nicht entnommen und der Aufrufer muss mindestens n
	/// Millisekunden warten.
	WartenZeit(usize),
	WartenPrio,
}

struct Einzeltempomat {
	geschw: isize,  // Bytes pro Sekunde
	reserve: isize, // Bytes
	max_reserve: isize,
	letztes_nachfüllen: Instant,
}
impl Einzeltempomat {
	fn melden(&mut self, bytes: isize) {
		self.reserve -= bytes;
		if self.reserve <= 0 {
			self.nachfüllen()
		}
	}

	// None bedeutet kein Warten nötig
	fn wartezeit_prüfen_ms(&mut self, bytes: isize) -> Option<isize> {
		if bytes > self.reserve {
			self.nachfüllen();
			// Wenn mehr Bytes gewünscht sind als wir in der Reserve haben können und
			// die Reserve voll ist lassen wir es zu.
			if self.reserve == self.max_reserve {
				return None;
			}

			Some(self.geschw * 1000 / bytes)
		} else {
			None
		}
	}

	fn nachfüllen(&mut self) {
		let vergangen = self.letztes_nachfüllen.elapsed().as_millis();
		if vergangen > 0 {
			self.letztes_nachfüllen = Instant::now();
			let vergangen_isize: isize = vergangen.try_into().unwrap();
			self.reserve += (self.geschw * vergangen_isize) / 1000;
			self.reserve = self.reserve.min(self.max_reserve);
		}
	}
}

struct EinzeltempomatSync {
	inneres: Mutex<Einzeltempomat>,
}
impl EinzeltempomatSync {
	fn neu(geschw: isize, max_reserve: isize) -> Self {
		EinzeltempomatSync {
			inneres: Mutex::new(Einzeltempomat {
				geschw,
				reserve: max_reserve,
				max_reserve,
				letztes_nachfüllen: Instant::now(),
			}),
		}
	}

	fn melden(&self, bytes: isize) {
		self.inneres.lock().unwrap().melden(bytes);
	}

	async fn warten(self: Arc<Self>, prio: Prio, bytes: isize) {
		loop {
			let warten_opt;
			{
				let mut guard = self.inneres.lock().unwrap();
				warten_opt = (*guard).wartezeit_prüfen_ms(bytes + prio.prio_bytes());
				if warten_opt.is_none() {
					(*guard).melden(bytes);
					return;
				}
			}

			let ms = warten_opt.unwrap();
			smol::Timer::after(Duration::from_millis(ms.try_into().unwrap())).await;
		}
	}
}

pub struct Tempomat {
	hoch: Arc<EinzeltempomatSync>,
	runter: Arc<EinzeltempomatSync>,
}
impl Tempomat {
	pub fn neu(
		geschw_hoch: isize,
		geschw_runter: isize,
		zeitrahmen_ms: isize,
	) -> Self {
		// Mindestens 6000 damit die Prioritäten funktionieren.
		let max_reserve_hoch = (zeitrahmen_ms * geschw_hoch / 1000).max(6000);
		let max_reserve_runter = (zeitrahmen_ms * geschw_runter / 1000).max(6000);

		Tempomat {
			hoch: Arc::new(EinzeltempomatSync::neu(geschw_hoch, max_reserve_hoch)),
			runter: Arc::new(EinzeltempomatSync::neu(
				geschw_hoch,
				max_reserve_runter,
			)),
		}
	}

	pub fn melden_hoch(&self, bytes: usize) {
		self.hoch.melden(bytes.try_into().unwrap());
	}

	pub fn melden_runter(&self, bytes: usize) {
		self.runter.melden(bytes.try_into().unwrap());
	}

	pub async fn warten_hoch(&self, prio: Prio, bytes: usize) {
		self
			.hoch
			.clone()
			.warten(prio, bytes.try_into().unwrap())
			.await;
	}

	pub async fn warten_runter(&self, prio: Prio, bytes: usize) {
		self
			.runter
			.clone()
			.warten(prio, bytes.try_into().unwrap())
			.await;
	}
}
