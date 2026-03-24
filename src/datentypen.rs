use std::ops::BitXor;
use std::fmt;
use serde_derive::*;
use smol::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use rand::random;
use crate::addr_generisch::Addr;

const BEP42_IPV4_MASKE : u32 = 0x030f3fff;
const BEP42_IPV6_MASKE : u64 = 0x0103070f1f3f7fff;

#[derive(Clone, PartialEq)]
pub struct KnotenInfo<A: Addr> {
	pub id: U160,
	pub addr: A
}


#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Serialize, Deserialize, Hash)]
pub struct U160(pub [u8;20]);
impl BitXor for U160 {
	type Output = Self;
	
	fn bitxor(self, rhs: Self) -> Self::Output {
		let mut entf = self.clone();
		entf.0.iter_mut().zip(rhs.0.iter()).for_each(|(a, b)| *a ^= b);
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
		self.bits()
			.zip(rhs.bits())
			.map(|(a,b)| a == b)
			.position(|b| !b)
			.unwrap_or(160)
	}
	
	pub fn bep42_generieren(ip: &smol::net::IpAddr) -> Self {
		let mut knoten_id : [u8;20] = random();
		let r : u8 = knoten_id[19] & 0x7;
		let crc = bep42_crc(ip, r);
		
		knoten_id[0] = (crc >> 24) as u8;
		knoten_id[1] = (crc >> 16) as u8;
		knoten_id[2] &= 0b00000111;
		knoten_id[2] |= (crc >> 8) as u8 & 0b11111000;
		U160(knoten_id)
	}
	
	pub fn bep42_prüfen(&self, ip: &IpAddr) -> bool {
		let r : u8 = (self.0[19] & 0x7).into();
		let crc = bep42_crc(ip, r);
		
		(crc >> 24) as u8 == self.0[0] &&
		(crc >> 16) as u8 == self.0[1] &&
		(crc >> 8) as u8 & 0b11111000 == self.0[2] & 0b11111000
	}
	
	pub fn zufällig() -> Self {
		let bytes : [u8;20] = random();
		U160(bytes)
	}
	
	/// Die ersten n bits werden beibehalten, aller bits danach werden
	/// invertiert.
	#[must_use]
	pub fn n_bits_beibehalten(&self, n: usize) -> U160 {
		let mut erg = self.clone();
		assert!(n < 20*8);
		if n == 20*8 { return erg; }
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

fn bep42_crc(ip:  &smol::net::IpAddr, r: u8) -> u32 {
	match ip {
		IpAddr::V4(ipv4) => {
			let maskierte_ip = ((ipv4.to_bits() & BEP42_IPV4_MASKE) | ((r as u32) << 29)).to_be_bytes();
			crc32c::crc32c(&maskierte_ip[..])
		},
		IpAddr::V6(ipv6) => {
			let maskierte_ip = (((ipv6.to_bits() >> 64) as u64 & BEP42_IPV6_MASKE) | ((r as u64) << 61)).to_be_bytes();
			crc32c::crc32c(&maskierte_ip[..])
		},
	}
}

pub struct U160BitIter<'a> {
	inner: &'a [u8],
	pos_in_byte: u8,
}

impl<'a> Iterator for U160BitIter<'a> {
	type Item = bool;
	
	fn next(&mut self) -> Option<Self::Item> {
		if self.inner.is_empty() { return None; }
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

	#[test]
	fn bep42() {
		for _ in 0..1000 {
			let ipv4_bytes : [u8;4] = random();
			let ipv6_bytes : [u8;16] = random();
			let ipv4 : IpAddr = Ipv4Addr::from(ipv4_bytes).into();
			let ipv6 : IpAddr = Ipv6Addr::from(ipv6_bytes).into();
			assert!(U160::bep42_generieren(&ipv4).bep42_prüfen(&ipv4));
			assert!(U160::bep42_generieren(&ipv6).bep42_prüfen(&ipv6));
		}
	}
}
