use crate::addr_generisch::Addr;
use rand::random;
use serde_derive::*;
use std::fmt;
use std::ops::BitXor;

#[derive(Clone, PartialEq)]
pub struct KnotenInfo<A: Addr> {
	pub id: U160,
	pub addr: A,
}

#[derive(
	PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Serialize, Deserialize, Hash,
)]
pub struct U160(pub [u8; 20]);
impl BitXor for U160 {
	type Output = Self;

	fn bitxor(self, rhs: Self) -> Self::Output {
		let mut entf = self.clone();
		entf
			.0
			.iter_mut()
			.zip(rhs.0.iter())
			.for_each(|(a, b)| *a ^= b);
		entf
	}
}

impl fmt::Display for U160 {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		for byte in self.0.iter() {
			write!(f, "{:02x?}", byte)?;
		}
		Ok(())
	}
}

impl fmt::Debug for U160 {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		<Self as fmt::Display>::fmt(self, f)
	}
}

impl U160 {
	pub fn bits<'a>(&'a self) -> U160BitIter<'a> {
		U160BitIter {
			inner: &self.0[..],
			pos_in_byte: 0,
		}
	}

	pub fn erstes_ungleiches_bit(&self, rhs: U160) -> usize {
		self
			.bits()
			.zip(rhs.bits())
			.map(|(a, b)| a == b)
			.position(|b| !b)
			.unwrap_or(160)
	}

	pub fn zufällig() -> Self {
		let bytes: [u8; 20] = random();
		U160(bytes)
	}

	/// Die ersten n bits werden beibehalten, aller bits danach werden
	/// invertiert.
	#[must_use]
	pub fn n_bits_beibehalten(&self, n: usize) -> U160 {
		let mut erg = self.clone();
		assert!(n < 20 * 8);
		if n == 20 * 8 {
			return erg;
		}
		let start_byte = n / 8;
		let rest_bits = n % 8;
		erg.0[start_byte] ^= 0b1111_1111 >> rest_bits;
		for byte in &mut erg.0[start_byte..] {
			*byte ^= 0b1111_1111;
		}

		erg
	}

	#[must_use]
	pub fn invertieren(&self) -> U160 {
		let mut erg = self.clone();
		for byte in &mut erg.0 {
			*byte ^= 0b1111_1111;
		}

		erg
	}
}

pub struct U160BitIter<'a> {
	inner: &'a [u8],
	pos_in_byte: u8,
}

impl<'a> Iterator for U160BitIter<'a> {
	type Item = bool;

	fn next(&mut self) -> Option<Self::Item> {
		if self.inner.is_empty() {
			return None;
		}
		let bit = (self.inner[0] >> self.pos_in_byte) & 1 == 1;
		self.pos_in_byte += 1;
		if self.pos_in_byte == 8 {
			self.inner = &self.inner[1..];
			self.pos_in_byte = 0;
		}
		Some(bit)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use smol::net::IpAddr;

	#[test]
	fn bep42() {
		for _ in 0..1000 {
			let ipv4_bytes: [u8; 4] = random();
			let ipv6_bytes: [u8; 16] = random();
			let ipv4: IpAddr = smol::net::Ipv4Addr::from(ipv4_bytes).into();
			let ipv6: IpAddr = smol::net::Ipv6Addr::from(ipv6_bytes).into();
			assert!(U160::bep42_generieren(&ipv4).bep42_prüfen(&ipv4));
			assert!(U160::bep42_generieren(&ipv6).bep42_prüfen(&ipv6));
		}
	}
}
