use super::*;
use std::borrow::{Borrow, Cow};

// Das Verhalten von bendy bezl. `Option`s ist nicht was wir brauchen, deswegen müssen wir einpaar Umwege machen.
pub fn ser_option<S: Serializer,T: Serialize>(d: &Option<T>, s: S) -> Result<S::Ok, S::Error>
{
	match d {
		Some(d2) => d2.serialize(s),
		// Muss mit #[serde(skip_serializing_if = "Option::is_none")] vorgefiltert werden.
		// Wir können diesen Fall nicht Sinvoll behandeln weil wir kein S::Ok konstruieren können.
		None => unreachable!(),
	}
}

pub fn de_option<'de, D: Deserializer<'de>, T: Deserialize<'de>>(de: D) -> Result<Option<T>, D::Error> {
	Ok(Some(T::deserialize(de)?))
}


pub struct MiniVec<T, const N: usize>(noalloc_vec_rs::vec::Vec<T, N>);
impl<T, const N: usize> MiniVec<T, N> {
	pub fn neu() -> Self {
		MiniVec(noalloc_vec_rs::vec::Vec::new())
	}
}

impl<T, const N: usize> Deref for MiniVec<T, N> {
	type Target = noalloc_vec_rs::vec::Vec<T, N>;
	
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}
impl<T, const N: usize> DerefMut for MiniVec<T, N> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.0
	}
}

impl<T: Serialize, const N: usize> Serialize for MiniVec<T, N> {
	fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
		let mut s_seq = s.serialize_seq(Some(self.len()))?;
		for e in self.iter() {
			s_seq.serialize_element(e)?;
		}
		s_seq.end()
	}
}

impl<'de, T: Deserialize<'de>, const N: usize> Deserialize<'de> for MiniVec<T, N> {
	fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
		de.deserialize_seq(MiniVecVisitor{
			_phantom_t: PhantomData::default(),
			_phantom_de: PhantomData::default(),
		})
	}
}

struct MiniVecVisitor<'de, T: Deserialize<'de>, const N: usize>{
	_phantom_t : PhantomData<T>,
	_phantom_de : PhantomData<&'de ()>,
}

impl<'de, T: Deserialize<'de>, const N: usize> serde::de::Visitor<'de> for MiniVecVisitor<'de, T,N> {
	type Value = MiniVec<T, N>;
	
	fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
		write!(formatter, "eine Liste mit {} oder weniger Elementen", N)
	}
	
	fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
		let mut v = MiniVec::neu();
		while let Some(e) = seq.next_element()? {
			if v.push(e).is_err() {
				return Err(A::Error::custom("Zu viele Elemente"));
			}
		}
		Ok(v)
	}
}

// ====================================================================================

pub struct KnotenListe<A: Addr> {
	pub daten: noalloc_vec_rs::vec::Vec<KnotenInfo<A>, K>
}
impl<A: Addr> Default for KnotenListe<A> {
	fn default() -> Self {
		KnotenListe { daten: noalloc_vec_rs::vec::Vec::new()}
	}
}
/*
impl<'a, A: Addr> ExactSizeIterator for KnotenListe<A> {}
impl<'a, A: Addr> Iterator for KnotenListe<A> {
	type Item = (A, U160);
	
	fn next(&mut self) -> Option<Self::Item> {
		if self.daten.len() == 0 { return None; }
		
		let mut knoten_id = [0u8;20];
		(&mut knoten_id[..]).copy_from_slice(&self.daten[0..20]);
		
		let element = (
			// unwrap: `A::aus_krpc_bytes` überprüft die Länge der Slice,
			// diese kann hier nicht falsch sein.
			A::aus_krpc_bytes(&self.daten[20..20+A::KRPC_LEN]).unwrap(),
			U160(knoten_id)
		);
		
		match &mut self.daten {
			Cow::Owned(d) => {
				unreachable!();
				// Wir benutzen `KnotenListe` mit `_.daten == Cow::Owned(_)` nur zu senden
				// und wollen die KnotenListe nie als Iterator benutzen.
				// d.drain(..20+A::KRPC_LEN);
			},
			Cow::Borrowed(d) => {
				*d = &d[20+A::KRPC_LEN..];
			}
		};
		
		Some(element)
	}
	
	fn size_hint(&self) -> (usize, Option<usize>) {
		let len = self.daten.len() / (A::KRPC_LEN + 20);
		(len, Some(len))
	}
}
*/


impl<A: Addr> KnotenListe<A> {
	fn neu() -> Self {
		Self::default()
	}
}

impl<A: Addr> Serialize for KnotenListe<A> {
	fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
		let mut puffer = Vec::with_capacity(K*(A::KRPC_LEN+20));
		
		for k_info in self.daten.iter() {
			puffer.extend_from_slice(k_info.id.0.as_slice());
			let mut tmp = vec![0u8;A::KRPC_LEN];
			k_info.addr.als_krpc_bytes(tmp.as_mut_slice());
			puffer.extend_from_slice(tmp.as_slice());
		}
		
		s.serialize_bytes(puffer.as_slice())
	}
}

impl<'de, A: Addr> Deserialize<'de> for KnotenListe<A> {
	fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
		
		de.deserialize_bytes(KnotenListeVisitor{
			_phantom_a: PhantomData::default(),
			_phantom_de: PhantomData::default(),
		})
	}
}

struct KnotenListeVisitor<'de, A: Addr>{
	_phantom_a : PhantomData<A>,
	_phantom_de : PhantomData<&'de ()>,
}

impl<'de, A: Addr> serde::de::Visitor<'de> for KnotenListeVisitor<'de, A> {
	type Value = KnotenListe<A>;
	
	fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
		write!(formatter, "bytes mit einer Länge % {} == 0", A::KRPC_LEN + 20)
	}
	
	fn visit_borrowed_bytes<E: serde::de::Error>(self, v: &'de [u8]) -> Result<Self::Value, E> {
		if v.len() % A::KRPC_LEN + 20 != 0 {
			return Err(E::invalid_length(v.len(),&"len % A::KRPC_LEN + 20 == 0"));
		}
		if v.len() > (A::KRPC_LEN + 20)*K {
			return Err(E::invalid_length(v.len(),&"len <= (A::KRPC_LEN + 20)*K"));
		}
		
		let mut daten = noalloc_vec_rs::vec::Vec::new();
		
		for rohdaten in v.chunks(A::KRPC_LEN+20) {
			daten.push(KnotenInfo {
				id: U160((&rohdaten[0..20]).try_into().unwrap()),
				addr: A::aus_krpc_bytes(&rohdaten[20..]).unwrap(),
			}).unwrap();
		}
		
		Ok(KnotenListe{ daten })
	}
}


// =================================================================


impl Serialize for super::KontaktInfoIpv4 {
	fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
		s.serialize_bytes(self.0.as_slice())
	}
}

// TODO Sollte DeserializeOwned sein. Der ganze Serde-Mist muss sowieso neu
//      geschrieben werden.
impl<'de> Deserialize<'de> for super::KontaktInfoIpv4 {
	fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
		de.deserialize_bytes(KontaktInfoIpv4Visitor{
			_phantom_de: PhantomData::default(),
		})
	}
}

struct KontaktInfoIpv4Visitor<'de>{
	_phantom_de : PhantomData<&'de ()>,
}

impl<'de> serde::de::Visitor<'de> for KontaktInfoIpv4Visitor<'de> {
	type Value = KontaktInfoIpv4;
	
	fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
		write!(formatter, "bytes mit einer Länge von {}", 6)
	}
	
	fn visit_borrowed_bytes<E: serde::de::Error>(self, v: &'de [u8]) -> Result<Self::Value, E> {
		if v.len() != 6 {
			return Err(E::invalid_length(v.len(),&"len == 6"));
		}
		
		Ok(KontaktInfoIpv4(v.try_into().unwrap()))
	}
}


impl Serialize for super::KontaktInfoIpv6 {
	fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
		s.serialize_bytes(self.0.as_slice())
	}
}

// TODO Sollte DeserializeOwned sein. Der ganze Serde-Mist muss sowieso neu
//      geschrieben werden.
impl<'de> Deserialize<'de> for super::KontaktInfoIpv6 {
	fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
		de.deserialize_bytes(KontaktInfoIpv6Visitor{
			_phantom_de: PhantomData::default(),
		})
	}
}

struct KontaktInfoIpv6Visitor<'de>{
	_phantom_de : PhantomData<&'de ()>,
}

impl<'de> serde::de::Visitor<'de> for KontaktInfoIpv6Visitor<'de> {
	type Value = KontaktInfoIpv6;
	
	fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
		write!(formatter, "bytes mit einer Länge von {}", 18)
	}
	
	fn visit_borrowed_bytes<E: serde::de::Error>(self, v: &'de [u8]) -> Result<Self::Value, E> {
		if v.len() != 18 {
			return Err(E::invalid_length(v.len(),&"len == 18"));
		}
		
		Ok(KontaktInfoIpv6(v.try_into().unwrap()))
	}
}

