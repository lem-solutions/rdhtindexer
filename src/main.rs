use std::sync::Arc;
use std::time::Duration;

use metrics::counter;
use metrics_exporter_prometheus::PrometheusBuilder;
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
	metriken();
	let tempomat = Arc::new(tempomat::Tempomat::neu(
		1024 * 1024 * 2, // 3MiB/s ↑
		1024 * 1024 * 4, // 5MiB/s ↓
		1000,            // 1s Toleranz
	));

	let (knoten_tx, knoten_rx) = smol::channel::unbounded();

	let knoten_fut = DhtKnoten::neu(
		smol::net::SocketAddrV4::new(std::net::Ipv4Addr::new(0, 0, 0, 0), 53722),
		tempomat,
		6000,
		Duration::from_secs(10),
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
		while let Some(_) = infos_pin.next().await {
			counter!("infohashes").increment(1);
		}
	});
}

fn metriken() {
	PrometheusBuilder::new()
		.with_http_listener(std::net::SocketAddrV4::new(
			std::net::Ipv4Addr::new(127, 0, 0, 1),
			8090,
		))
		.install()
		.unwrap();
}
