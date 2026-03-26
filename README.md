# rDHTindexer

Indexer/Crawler für das Bittorrent DHT-Netzwerk. Unfertig und noch nicht Einsatzfähig.

## TODOs

- [x] Nach Info-Hashes scannen
- [ ] Verfnünftige implementierung von `DhtKnoten::knoten_iterativ_suchen`
- [ ] Runterladen der Metadaten
- [ ] Datenbankschema und einfügen der Ergebnise in selbiges
- [ ] Frontend

## Relevente BEPs

- [BEP 05: DHT Protocol](https://bittorrent.org/beps/bep_0005.html)
- [BEP 32: BitTorrent DHT Extensions for IPv6](https://bittorrent.org/beps/bep_0032.html)
	- Unterstützung für Hybridlisten nicht implementiert.
- [BEP 42: DHT Security Extension](https://bittorrent.org/beps/bep_0042.html)
- [BEP 51: DHT Infohash Indexing](https://bittorrent.org/beps/bep_0051.html)

### Nicht implementiert

- [BEP 33: DHT scrape](https://bittorrent.org/beps/bep_0033.html)
- [BEP 43: Read-only DHT Nodes](https://bittorrent.org/beps/bep_0043.html)
