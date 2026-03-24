use crate::krpc::Will;
use smol::net::*;
use std::hash::Hash;

// TODO Es wäre ideomatischer wenn wir eigene Größe-Null-Datentypen definieren
//      um `Addr` für diese zu implementieren.

/// Wir benutzen diesen Trait um den Code für IPv4 und IPv6 zu generalisieren.
/// Da mehrere Datentypen involviert sind ist es eigentlich egal ob wir den
/// Trait für SocketAddrV* oder für krpc::KontaktInfoIpv* implementieren.
pub trait Addr:
	smol::net::AsyncToSocketAddrs
	+ Into<SocketAddr>
	+ std::fmt::Display
	+ Clone
	+ PartialEq
	+ Hash
	+ Send
	+ Sync
{
	// type Krpc : Serialize + for<'de> Deserialize<'de> + PartialEq + Clone;
	const KRPC_LEN: usize;
	const IST_IPV4: bool;

	fn ip(&self) -> IpAddr;
	fn port(&self) -> u16;
	fn port_ändern(&mut self, port: u16);
	// fn als_krpc(&self) -> Self::Krpc;
	fn aus_socket_addr(addr: SocketAddr) -> Option<Self>;
	fn aus_krpc_bytes(bytes: &[u8]) -> Option<Self>;
	fn als_krpc_bytes(&self, puffer: &mut [u8]);
	// fn aus_krpc(krpc: &Self::Krpc) -> Self;
	fn global_valide(&self) -> bool;

	fn als_ipv4(&self) -> Option<&SocketAddrV4>;
	fn als_ipv6(&self) -> Option<&SocketAddrV6>;

	fn will_opt(will: Option<Will>) -> Will;

	fn als_socket_addr(&self) -> SocketAddr {
		if let Some(a) = self.als_ipv4() {
			return (*a).into();
		}

		if let Some(a) = self.als_ipv6() {
			return (*a).into();
		}

		unreachable!();
	}
}

impl Addr for SocketAddrV4 {
	// type Krpc = krpc::KontaktInfoIpv4;
	const KRPC_LEN: usize = 6;
	const IST_IPV4: bool = true;

	fn ip(&self) -> IpAddr {
		(*self.ip()).into()
	}

	fn port(&self) -> u16 {
		self.port()
	}

	fn port_ändern(&mut self, port: u16) {
		self.set_port(port);
	}

	fn aus_socket_addr(a: SocketAddr) -> Option<Self> {
		match a {
			SocketAddr::V4(x) => Some(x),
			SocketAddr::V6(_) => None,
		}
	}

	/*
	fn als_krpc(&self) -> Self::Krpc {
			let mut krpc = krpc::KontaktInfoIpv4([0u8;6]);
			(&mut krpc.0[0..4]).copy_from_slice(&self.ip().octets()[..]);
			(&mut krpc.0[4..6]).copy_from_slice(&self.port().to_be_bytes()[..]);
			krpc
	}
	*/

	fn aus_krpc_bytes(bytes: &[u8]) -> Option<Self> {
		if bytes.len() != Self::KRPC_LEN {
			return None;
		}
		let mut ip_bytes = [0u8; 4];
		(&mut ip_bytes[..]).copy_from_slice(&bytes[0..4]);
		let mut port_bytes = [0u8; 2];
		(&mut port_bytes[..]).copy_from_slice(&bytes[4..6]);
		let port = u16::from_be_bytes(port_bytes);
		Some(SocketAddrV4::new(Ipv4Addr::from_octets(ip_bytes), port))
	}

	fn als_krpc_bytes(&self, puffer: &mut [u8]) {
		assert_eq!(puffer.len(), Self::KRPC_LEN);
		(&mut puffer[0..4]).copy_from_slice(&self.ip().octets()[..]);
		(&mut puffer[4..6]).copy_from_slice(&self.port().to_be_bytes()[..]);
	}

	/*
	fn aus_krpc(krpc: &Self::Krpc) -> Self {
			// unwrap: `Self::aus_krpc_bytes` überprüft nur die Länge der Slice,
			// bei einem `Self::Krpc` muss die Länge bereits korrekt sein.
			Self::aus_krpc_bytes(&krpc.0[..]).unwrap()
	}
	*/

	#[cfg(feature = "ips_validieren")]
	fn global_valide(&self) -> bool {
		self.is_global() && !self.is_multicast()
	}

	#[cfg(not(feature = "ips_validieren"))]
	fn global_valide(&self) -> bool {
		true
	}

	fn als_ipv4(&self) -> Option<&SocketAddrV4> {
		Some(self)
	}

	fn als_ipv6(&self) -> Option<&SocketAddrV6> {
		None
	}

	fn will_opt(will: Option<Will>) -> Will {
		will.unwrap_or(Will::Ipv4)
	}
}

impl Addr for SocketAddrV6 {
	const KRPC_LEN: usize = 18;
	// type Krpc = krpc::KontaktInfoIpv6;
	const IST_IPV4: bool = false;

	fn ip(&self) -> IpAddr {
		(*self.ip()).into()
	}

	fn port(&self) -> u16 {
		self.port()
	}

	fn port_ändern(&mut self, port: u16) {
		self.set_port(port);
	}

	fn aus_socket_addr(a: SocketAddr) -> Option<Self> {
		match a {
			SocketAddr::V4(_) => None,
			SocketAddr::V6(x) => Some(x),
		}
	}

	fn aus_krpc_bytes(bytes: &[u8]) -> Option<Self> {
		if bytes.len() != Self::KRPC_LEN {
			return None;
		}
		let mut ip_bytes = [0u8; 16];
		(&mut ip_bytes[..]).copy_from_slice(&bytes[0..16]);
		let mut port_bytes = [0u8; 2];
		(&mut port_bytes[..]).copy_from_slice(&bytes[16..18]);
		let port = u16::from_be_bytes(port_bytes);
		Some(SocketAddrV6::new(
			Ipv6Addr::from_octets(ip_bytes),
			port,
			0,
			0,
		))
	}

	fn als_krpc_bytes(&self, puffer: &mut [u8]) {
		assert_eq!(puffer.len(), Self::KRPC_LEN);
		(&mut puffer[0..16]).copy_from_slice(&self.ip().octets()[..]);
		(&mut puffer[16..18]).copy_from_slice(&self.port().to_be_bytes()[..]);
	}

	#[cfg(feature = "ips_validieren")]
	fn global_valide(&self) -> bool {
		self.is_unicast_global()
	}

	#[cfg(not(feature = "ips_validieren"))]
	fn global_valide(&self) -> bool {
		true
	}

	fn als_ipv4(&self) -> Option<&SocketAddrV4> {
		None
	}

	fn als_ipv6(&self) -> Option<&SocketAddrV6> {
		Some(self)
	}

	fn will_opt(will: Option<Will>) -> Will {
		will.unwrap_or(Will::Ipv6)
	}
}
