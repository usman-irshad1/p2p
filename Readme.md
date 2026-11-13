# P2P UDP Group Chat and File Sharing

A student project to build a multi-peer group chat and file-sharing application over UDP, without a permanent central server. A running peer can act as the bootstrap contact for newcomers.

## Current project checklist

### Implemented or prototyped

- [x] Python UDP socket setup and command-line local port/bootstrap address.
- [x] Peer ID generation using a 16-bit random value.
- [x] Bootstrap join message and JOIN_ACK response containing known peer IDs, IP addresses, and ports.
- [x] `new_peer` notification so existing peers can add a newcomer to their peer lists.
- [x] A 20-byte application header format using network byte order: `!HHBBHIHHHH`.
- [x] Packet type IDs assigned for JOIN, CHAT, CHUNK_REQUEST, LEAVE, HEARTBEAT, ACK, JOIN_ACK, CHUNK_DATA, FILE_LIST, CHUNK_HAVE, and REPLICATE_REQ.
- [x] Payload helpers/prototypes for chat text, chunk requests, chunk data fragments, and chunk availability.
- [x] SHA-256 file ID helper.
- [x] Local CHUNK_DATA fragmentation/reassembly demonstration: two 500-byte fragments reassembled into the original 1,000-byte chunk.
- [x] Local peer discovery/heartbeat demonstration with three localhost peers.
- [x] Local timeout demonstration showing a silent peer removed after its heartbeat timeout.

### Still to implement or integrate

- [ ] **Sync the tested heartbeat timeout implementation to GitHub.** The current pushed `p2p_file.py` sends heartbeats and parses their IDs, but does not update `last_seen` on heartbeat receipt or remove timed-out peers. Local screenshots show the timeout behavior, so commit and push that tested version.
- [ ] Add a graceful LEAVE message and remove the leaving peer immediately.
- [ ] Integrate the binary packet header and packet type handling from `protocol_design.py` into the live UDP peer program. Peer discovery currently uses text messages.
- [ ] Implement reliable group chat over UDP: per-peer/directional sequence tracking, acknowledgments, retransmission, duplicate suppression, and in-order delivery.
- [ ] Implement a real file catalog and chunk ownership announcements.
- [ ] Transfer actual file chunks between peers, save received chunks, reassemble complete files, and verify file integrity.
- [ ] Store each chunk on at least two peers to meet the project's redundancy requirement.
- [ ] Implement multi-source downloads that fetch chunks from at least two peers in parallel.
- [ ] Test failover by stopping a chunk source during a download and completing from another replica.
- [ ] Test with at least four peers and record measured results/graphs for the project report.
- [ ] Package the report, source code, and run instructions for submission and prepare the live demonstration.

## Run the current localhost peer discovery prototype

Open separate terminals in the project folder.

First peer:

```powershell
python p2p_file.py 5000
```

Additional peers:

```powershell
python p2p_file.py 5001 127.0.0.1:5000
python p2p_file.py 5002 127.0.0.1:5000
```

These commands are for testing multiple peers on one computer. The code's advertised address is currently `127.0.0.1`; peers on separate computers need a reachable LAN address instead.

## Current limitations

- The discovery program and binary protocol prototype are separate files and are not yet one integrated application.
- UDP does not guarantee delivery. The chat and file-transfer reliability features remain to be implemented.
- The peer list is currently in memory; it is not persistent.
- Localhost tests do not establish that peers can connect across different networks or through NAT.

## Evidence log

Debugging screenshots and local test results are being collected in the running PDF: `p2p_debug_outputs.pdf`.
