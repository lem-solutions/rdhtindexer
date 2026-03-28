use std::time::{Duration, Instant};

/// Minimale Wartezeit bevor ein Knoten neu gescannt werden darf. Muss
/// mindestens 6 Stunden sein.
const ZEITGRENZE: Duration = Duration::from_hours(6);

use bloomfilter::Bloom;

use crate::datentypen::U160;

const BLOOM_FALSCH_POSITIV_RATE: f64 = 0.05;
const BLOOM_SCHÄTZUNG_EINTRÄGE_PRO_ZEITGRENZE: usize = 15_000_000;

pub struct KnotenSperrliste {
	erledigt_a: Bloom<U160>,
	erledigt_b: Option<Bloom<U160>>,
	anz_erledigt_a: usize,
	zeitstempel: Instant,
}
impl KnotenSperrliste {
	pub fn neu() -> Self {
		KnotenSperrliste {
			erledigt_a: Bloom::new_for_fp_rate(
				BLOOM_SCHÄTZUNG_EINTRÄGE_PRO_ZEITGRENZE,
				BLOOM_FALSCH_POSITIV_RATE,
			)
			.unwrap(),
			erledigt_b: None,
			zeitstempel: Instant::now(),
			anz_erledigt_a: 0,
		}
	}

	/// Fügt die gegebene Knoten-Id ein und gibt zurück ob diese bereits vorhanden
	/// war.
	pub fn gesehen(&mut self, id: U160) -> bool {
		// TODO Diese Schätzung ist nicht ganz genau, da faktisch mehr
		//      verschiedene ID gesehen wurden als gezählt(wegen der
		//      Falsch-Positiv Rate). Man könnte ausrechnen wie hoch diese
		//      statistisch sein müsste.
		if self.zeitstempel.elapsed() > ZEITGRENZE {
			self.erledigt_b = Some(
				Bloom::new_for_fp_rate(self.anz_erledigt_a, BLOOM_FALSCH_POSITIV_RATE)
					.unwrap(),
			);
			std::mem::swap(&mut self.erledigt_a, self.erledigt_b.as_mut().unwrap());

			self.anz_erledigt_a = 0;
			self.zeitstempel = Instant::now();
		}

		// b zuerst, da wie die ID nicht in a einfügen wollen, falls sie schon in b
		// ist.
		if let Some(ref b) = self.erledigt_b {
			return b.check(&id);
		}

		if self.erledigt_a.check_and_set(&id) {
			return true;
		} else {
			self.anz_erledigt_a += 1;
		}

		false
	}
}
