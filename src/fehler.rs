use thiserror::Error;

#[derive(Error, Debug)]
pub enum Fehler {
	#[error("Bencode Fehler(Dekodieren): {0}")]
	BencodeDeFehler(#[from] bendy::decoding::Error),

	#[error("Bencode Fehler(Kodieren): {0}")]
	BencodeSerFehler(#[from] bendy::encoding::Error),

	#[error("E/A-Fehler: {0}")]
	EaFehler(#[from] std::io::Error),

	#[error("interner Kanalfehler: {0}")]
	OneshotRecvFehler(#[from] oneshot::RecvError),

	#[error("Beschränkung für gleichzeitige Anfragen überschritten")]
	ZuVieleAnfragen,

	#[error("Wir haben versucht ein zu großes KRPC-Paket zu senden ({0})")]
	GesendeteNachrichtZuLang(usize),
}
