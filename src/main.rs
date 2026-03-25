use std::sync::Arc;
use std::time::Duration;

use smol::stream::StreamExt;

mod addr_generisch;
mod datentypen;
mod dht_knoten;
mod fehler;
mod krpc;
mod scanner;
mod tempomat;

pub(crate) use fehler::*;

use dht_knoten::DhtKnoten;

use crate::scanner::Scanner;

fn main() {
	env_logger::init();
	let tempomat = Arc::new(tempomat::Tempomat::neu());

	let (knoten_tx, knoten_rx) = smol::channel::unbounded();

	let knoten_fut = DhtKnoten::neu(
		smol::net::SocketAddrV4::new(std::net::Ipv4Addr::new(0, 0, 0, 0), 53722),
		tempomat,
		128,
		Duration::from_secs(30),
		1024,
		Duration::from_hours(6),
		Box::new(|_| {}),
		Box::new(|_| {}),
		Box::new(move |k| {
			knoten_tx.force_send(k).unwrap();
		}),
	);
	let knoten = Arc::new(smol::block_on(knoten_fut).unwrap());

	let bootstrap = vec![smol::net::SocketAddrV4::new(
		std::net::Ipv4Addr::new(212, 129, 33, 59),
		6881,
	)];
	knoten.clone().starten(bootstrap);
	let scanner = Arc::new(Scanner::neu(knoten, knoten_rx));
	let infos = scanner.scannen();

	smol::block_on(async {
		let mut infos_pin = std::pin::pin!(infos);
		while let Some(info) = infos_pin.next().await {
			let info_hash = info.info_hash;
			let addr = info.addr;
			log::info!("INFOHASH {info_hash} {addr}");
		}
	});
}
