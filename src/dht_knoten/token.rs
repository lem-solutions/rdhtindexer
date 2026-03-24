use crate::addr_generisch::Addr;
use crate::datentypen::U160;
use lazy_static::lazy_static;
use std::hash::{DefaultHasher, Hash, Hasher};

const TOKEN_ZEITFENSTER_SEK: u64 = 60 * 5;

// Siehe https://stackoverflow.com/questions/75390300/how-is-a-token-value-generated-in-mainline-dhts-get-peers-query

// TODO evlt. eine kryptographisch sichere Hashfunktion benutzen.
lazy_static! {
		// Theoretisch sollte dieser Wert pro DHT Knoten verschieden sein, aber das
		// ist eigentlich egal.
		static ref geheim : [u8;16] = rand::random();
}

pub fn token_überprüfen<A: Addr>(
	knoten_id: U160,
	addr: &A,
	token: &[u8],
) -> bool {
	token == token_generieren_intern(knoten_id, addr, false).as_slice()
		|| token == token_generieren_intern(knoten_id, addr, true).as_slice()
}

pub fn token_generieren<A: Addr>(knoten_id: U160, addr: &A) -> [u8; 8] {
	token_generieren_intern(knoten_id, addr, false)
}

fn token_generieren_intern<A: Addr>(
	knoten_id: U160,
	addr: &A,
	letztes_zeitfenster: bool,
) -> [u8; 8] {
	let mut hasher = DefaultHasher::default();
	hasher.write(geheim.as_slice());
	knoten_id.hash(&mut hasher);
	addr.hash(&mut hasher);
	let mut zeitfenster =
		std::time::UNIX_EPOCH.elapsed().unwrap().as_secs() / TOKEN_ZEITFENSTER_SEK;
	if letztes_zeitfenster {
		zeitfenster = zeitfenster.saturating_sub(1);
	}
	hasher.write_u64(zeitfenster);

	hasher.finish().to_be_bytes()
}
