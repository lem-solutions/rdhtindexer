use std::net::UdpSocket;
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


/*
struct DhtKnoten {
	routing_tabelle: RoutingTabelle,
	socket: UdpSocket,
}
*/
fn main() {
	env_logger::init();
	/*
	smol::block_on(async {
		let mut stream = net::TcpStream::connect("example.com:80").await?;
		let req = b"GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n";
		stream.write_all(req).await?;

		let mut stdout = Unblock::new(std::io::stdout());
		io::copy(stream, &mut stdout).await?;
		Ok(())
	})
	*/
	
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
	
	//let async_exec = smol::LocalExecutor::new();
	
	let bootstrap = vec![
		smol::net::SocketAddrV4::new(std::net::Ipv4Addr::new(212,129,33,59), 6881)
	];
	knoten.clone().starten(bootstrap);
	
	
	let scanner = Arc::new(Scanner::neu(knoten, knoten_empfänger.knoten));
	let infos = scanner.scannen();
	
	
	
	/*
	loop {
		smol::future::block_on(async_exec.tick());
	}
	*/
	
	smol::block_on(async {
		let mut infos_pin = std::pin::pin!(infos);
		while let Some(info) = infos_pin.next().await {
			let info_hash = info.info_hash;
			let addr = info.addr;
			log::info!("INFOHASH {info_hash} {addr}");
		}
	});
	
	/*
	loop {
		smol::future::block_on(async_exec.tick());
	}
	*/
	
	
}

async fn ignorieren<T>(rx: smol::channel::Receiver<T>) {
	let mut rx_pin = std::pin::pin!(rx);
	loop {
		if rx_pin.next().await.is_none() { return; }
	}
}

/*
fn _rx_print() {
	let s = UdpSocket::bind("[::]:53722").unwrap();
	let mut buf = [0u8;4096];
	loop {
		let (len, src) = s.recv_from(&mut buf[..]).unwrap();
		match bendy::serde::from_bytes::<krpc::KrpcNachricht<smol::net::SocketAddrV6>>(&buf[..len]) {
			Err(e) => println!("{src} {len} {e}\n{}", String::from_utf8_lossy(&buf[..len])),
			Ok(n) => println!("{src} {len} {:?} {:?}", n.art, n.anfrage_methode),
		}
	}
}
*/
