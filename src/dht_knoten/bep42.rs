use crate::datentypen::U160;
use rand::random;
use smol::net::IpAddr;
use std::collections::HashSet;

const BEP42_IPV4_MASKE: u32 = 0x030f3fff;
const BEP42_IPV6_MASKE: u64 = 0x0103070f1f3f7fff;

/// Wenn wir von so vielen verschiedenen Knoten (mit verschiedenen IPs)
/// übereinstimmend über über unsere eigene externe IP informiert wurden passen
/// wir unsere Knoten-ID an.
const ANZ_IPS_FÜR_IP_WECHSEL: usize = 20;

pub struct ExtIpPrüfer {
	aktuelle_ip: Option<IpAddr>,
	neue_ip: Option<IpAddr>,
	neue_ip_von: HashSet<IpAddr>,
}
impl ExtIpPrüfer {
	pub fn neu() -> Self {
		ExtIpPrüfer {
			aktuelle_ip: None,
			neue_ip: None,
			neue_ip_von: HashSet::new(),
		}
	}

	/// Nimmt eine Behauptung über unsere externe IP-Addresse zur Kenntnis und
	/// gibt zurück ob wir aufgrund der Behauptung unsere Knoten-ID anpassen
	/// sollten.
	pub fn ip_wechsel_prüfen(
		&mut self,
		behauptung_von: IpAddr,
		angebliche_ext_ip: IpAddr,
	) -> bool {
		match (self.aktuelle_ip, self.neue_ip) {
			(Some(aktuell), _) if aktuell == angebliche_ext_ip => {
				self.neue_ip = None;
				self.neue_ip_von.clear();
				false
			}
			(Some(_), None) => {
				self.neue_ip = Some(angebliche_ext_ip);
				self.neue_ip_von.insert(behauptung_von);
				false
			}
			(Some(_), Some(bisherige_behauptung))
				if bisherige_behauptung == angebliche_ext_ip =>
			{
				self.neue_ip_von.insert(behauptung_von);
				if self.neue_ip_von.len() >= ANZ_IPS_FÜR_IP_WECHSEL {
					self.aktuelle_ip = Some(angebliche_ext_ip);
					self.neue_ip = None;
					self.neue_ip_von.clear();
					true
				} else {
					self.neue_ip_von.insert(behauptung_von);
					false
				}
			}
			(Some(_), Some(_)) => {
				self.neue_ip = Some(angebliche_ext_ip);
				self.neue_ip_von.clear();
				self.neue_ip_von.insert(behauptung_von);
				false
			}
			(None, neu) => {
				assert!(neu.is_none());
				self.aktuelle_ip = Some(angebliche_ext_ip);
				true
			}
		}
	}
}

impl U160 {
	pub fn bep42_generieren(ip: &smol::net::IpAddr) -> Self {
		let mut knoten_id: [u8; 20] = random();
		let r: u8 = knoten_id[19] & 0x7;
		let crc = bep42_crc(ip, r);

		knoten_id[0] = (crc >> 24) as u8;
		knoten_id[1] = (crc >> 16) as u8;
		knoten_id[2] &= 0b00000111;
		knoten_id[2] |= (crc >> 8) as u8 & 0b11111000;
		U160(knoten_id)
	}

	pub fn bep42_prüfen(&self, ip: &IpAddr) -> bool {
		let r: u8 = (self.0[19] & 0x7).into();
		let crc = bep42_crc(ip, r);

		(crc >> 24) as u8 == self.0[0]
			&& (crc >> 16) as u8 == self.0[1]
			&& (crc >> 8) as u8 & 0b11111000 == self.0[2] & 0b11111000
	}
}

fn bep42_crc(ip: &smol::net::IpAddr, r: u8) -> u32 {
	match ip {
		IpAddr::V4(ipv4) => {
			let maskierte_ip = ((ipv4.to_bits() & BEP42_IPV4_MASKE)
				| ((r as u32) << 29))
				.to_be_bytes();
			crc32c::crc32c(&maskierte_ip[..])
		}
		IpAddr::V6(ipv6) => {
			let maskierte_ip = (((ipv6.to_bits() >> 64) as u64 & BEP42_IPV6_MASKE)
				| ((r as u64) << 61))
				.to_be_bytes();
			crc32c::crc32c(&maskierte_ip[..])
		}
	}
}
