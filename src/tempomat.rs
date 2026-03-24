pub struct Tempomat();
impl Tempomat {
	pub fn neu() -> Self {
		Tempomat()
	}

	pub fn melden_hoch(&self, _aufgabe: &'static str, _bytes: usize) {}

	pub fn melden_runter(&self, _aufgabe: &'static str, _bytes: usize) {}

	pub async fn warten_hoch(&self, _aufgabe: &'static str, _bytes: usize) {}

	pub async fn warten_runter(&self, _aufgabe: &'static str) {}
}
