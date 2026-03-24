use std::sync::Arc;
use std::time::Duration;

use smol::stream::StreamExt;

mod datentypen;
mod krpc;
mod tempomat;
mod dht_knoten;
mod fehler;
mod addr_generisch;
mod scanner;

pub(crate) use fehler::*;

use dht_knoten::DhtKnoten;

use crate::dht_knoten::KnotenKanäle;
use crate::scanner::Scanner;

fn main() {
	env_logger::init();
	
	let tempomat = Arc::new(tempomat::Tempomat::neu());
	let (knoten_kanäle, knoten_empfänger) = KnotenKanäle::neu(100);
	
	let knoten_fut = DhtKnoten::neu(
		smol::net::SocketAddrV4::new(std::net::Ipv4Addr::new(0,0,0,0), 53722),
		tempomat,
		knoten_kanäle,
		128,
		Duration::from_secs(30),
		1024,
		Duration::from_hours(6),
	);
	let knoten = Arc::new(smol::block_on(knoten_fut).unwrap());
	
	std::thread::spawn(
		|| {
			smol::block_on(async {
				let smol_rt = smol::LocalExecutor::new();
				// smol_rt.spawn(ignorieren(knoten_empfänger.knoten)).detach();
				smol_rt.spawn(ignorieren(knoten_empfänger.info_hash_mit_peer)).detach();
				smol_rt.spawn(ignorieren(knoten_empfänger.info_hash_mit_knoten)).detach();
				loop {
					smol_rt.tick().await;
				}
			})
		}
	);
	
	let bootstrap = vec![
		smol::net::SocketAddrV4::new(std::net::Ipv4Addr::new(212,129,33,59), 6881)
	];
	knoten.clone().starten(bootstrap);
	let scanner = Arc::new(Scanner::neu(knoten, knoten_empfänger.knoten));
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

async fn ignorieren<T>(rx: smol::channel::Receiver<T>) {
	let mut rx_pin = std::pin::pin!(rx);
	loop {
		if rx_pin.next().await.is_none() { return; }
	}
}
