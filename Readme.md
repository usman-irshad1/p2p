# P2P UDP Group Chat and File Sharing

A student project to build a multi-peer group chat and file-sharing application over UDP. Peers can use a bootstrap peer to discover the group; the bootstrap peer is not intended to remain a central server.

## Project status

**Status:** The supplied unit suite passed (17 tests), and the expanded localhost integration run passed on 25 Nov 2026. The run verified four-peer chat and membership, five-peer late-join rebalancing, replicated file placement, multi-source download, corruption fallback, and recovery after a source crash. The late-join transfer now retries with a refreshed owner list. Real LAN interoperability and broader packet-loss simulation remain unverified.

### Progress against `protocol_improvements.pdf`

**Implementation: 5/5 numbered prototype changes present in code.** This count covers the PDF's five-item change list, not every general option or future improvement discussed elsewhere in the study. The supplied integration run verified replica placement, multi-source download, and crash recovery on localhost. It did not simulate random packet loss.

1. [x] Selective `CHUNK_ACK`: 32-fragment windows; retransmit fragments that remain unacknowledged.
2. [x] Pace `CHUNK_DATA` bursts and request a 4 MiB UDP receive buffer.
3. [x] Adaptive per-peer RTO, failover after three RTOs without progress, and a sliding pool that starts another chunk when a worker finishes.
4. [x] Ranged `CHUNK_REQUEST` with resume offsets, `CHUNK_NAK` failover, and per-chunk SHA-256 metadata/checking (`CHUNK_HASH`).
5. [x] Bootstrap ID assignment, heartbeat membership-view hashes and repair, and bounded HRW placement across at least three peers.

These items are present in the current code. The localhost integration run verifies the main flows described below; LAN interoperability and broader packet-loss tests remain outstanding.

### Implemented or prototyped

- [x] UDP peer startup with a command-line port and optional bootstrap address.
- [x] Peer IDs, join, peer-list response, and new-peer notifications.
- [x] Heartbeats, peer timeout removal, and graceful LEAVE handling.
- [x] A 20-byte application header using network byte order: `!HHBBHIHHHH`.
- [x] Packet type IDs and payload helpers for JOIN, CHAT, ranged CHUNK_REQUEST, LEAVE, HEARTBEAT, ACK, JOIN_ACK, CHUNK_DATA, FILE_LIST, FILE_LIST_ACK, CHUNK_HAVE, CHUNK_HAVE_ACK, REPLICATE_REQ, NEW_PEER, CHUNK_ACK, CHUNK_NAK, CHUNK_HASH, CHUNK_HASH_ACK, and PEER_LIST_REQ.
- [x] CHUNK_ACK selective acknowledgements now drive 32-fragment send windows with bounded retransmission; CHUNK_NAK reports missing or corrupt chunks and lets downloads try another owner.
- [x] Peers exchange a SHA-256 digest for each chunk; incoming chunks are checked before they are stored or advertised.
- [x] CHUNK_HASH delivery retries the same digest packet after one second, up to five times, until the matching CHUNK_HASH_ACK arrives.
- [x] FILE_LIST records are acknowledged per file and retried after one second, up to five times, if the FILE_LIST_ACK is lost.
- [x] CHUNK_HAVE ownership announcements are acknowledged and retried after one second, up to five times, if the matching CHUNK_HAVE_ACK is lost.
- [x] Checksum and packet validation helpers, including CHAT and ACK validation.
- [x] Local CHUNK_DATA fragmentation and reassembly demonstration: two 500-byte fragments rebuild a 1,000-byte chunk.
- [x] Reliable ordered chat logic in `p2p_file.py`: per-peer queues, exact ACK matching, an eight-packet window, adaptive retransmission timing, duplicate suppression, and in-order acceptance.
- [x] Version 2 chat session IDs let a sender restart its per-peer counters after membership removal without being mistaken for an old duplicate.
- [x] In-memory checks passed for Rules 6 and 7: duplicates are ACKed but not displayed twice, out-of-order messages are rejected, and peer removal clears chat and sequence state.
- [x] Live FILE_LIST and CHUNK_HAVE catalog exchange on peer join, plus `/files` to inspect known metadata and chunk owners.
- [x] `chunk_store.py` writes individual chunks to disk and rebuilds a file in order with a full-file SHA-256 check.
- [x] `/index <path>` splits a file into 256 KiB chunks; bounded HRW placement selects two distinct owners per chunk from at least three known peers and avoids assigning every chunk to one peer.
- [x] `/download <file_id_hex> [output_path]` uses a sliding pool of up to four chunk workers, prefers rare chunks, caps planned concurrent downloads at two per source, and verifies the rebuilt file's SHA-256.
- [x] User downloads use a private staging folder and do not advertise all downloaded chunks as owned replicas.
- [x] Each local peer gets a separate chunk directory (`peer_chunks/<port>/`); `P2P_STORE_DIR` can select another base directory.
- [x] CHUNK_REQUEST carries a start offset and length so a download can resume at the first missing byte; a source change is attempted after three adaptive RTOs without progress.
- [x] CHUNK_DATA transmission pauses briefly after each eight-fragment burst; the UDP socket requests a 4 MiB receive buffer (the operating system may clamp this value).
- [x] Live CHUNK_ACK/CHUNK_NAK handling: receivers report cumulative progress and selective fragment receipt; each 32-fragment window gets up to five send rounds, and a missing chunk triggers NOT_STORED.
- [x] Chunk serving accepts up to four simultaneous outgoing transfers and replies with CHUNK_NAK/BUSY when all slots are occupied, allowing the downloader to try another owner.
- [x] On LEAVE or heartbeat timeout, the lowest-ID surviving owner asks an unassigned peer to copy an under-replicated chunk; repair requests retry until a replacement owner is announced.
- [x] When a peer joins, it computes its assigned share and fetches missing chunks from known owners; old replicas are retained, so this is additive rebalancing.
- [x] Heartbeats include a CRC-32 membership-view hash; two consecutive mismatches trigger a binary PEER_LIST_REQ and JOIN_ACK snapshot exchange.
- [x] Bootstrap peers assign joining peer IDs from their currently known active IDs.
- [x] Membership uses binary JOIN, JOIN_ACK, NEW_PEER, HEARTBEAT, LEAVE, and PEER_LIST_REQ datagrams; JOIN is retried and deduplicated by UDP source address.
- [x] Bootstrap names are resolved, the advertised LAN address is detected or can be set with `--ip`, and JOIN_ACK peer lists are limited to one datagram.
- [x] Initial replica requests are retried, failed downloads retry chunks, and re-replication chooses a candidate while avoiding a complete-file copy.
- [x] Background chat and heartbeat loops continue after a handled send/processing error. Windows UDP ICMP reset errors are disabled when supported.
- [x] Local screenshots show peers joining and receiving sequential chat messages. Evidence is collected in [the debugging PDF](evidence/p2p_debug_outputs.pdf).

### Remaining for the full assignment

- [ ] Simulate packet loss across FILE_LIST, CHUNK_HASH, and CHUNK_HAVE retries; the integration test deliberately drops one CHUNK_DATA fragment and verifies retransmission.
- [x] Corrupt a stored chunk and verify hash rejection and fallback to a valid replica in the localhost integration run.
- [x] Exercise a simulated BUSY response during replication in the localhost integration run; saturation of all four real transfer slots still needs a separate test.
- [x] Four-peer localhost checks passed for membership, chat, two 2.5 MB files, replica count, and no complete-file owner.
- [x] Fifth-peer late join passed: the new peer received every chunk assigned to it under the updated placement.
- [x] The integration run indexed two 2.5 MB files from each of five peers.
- [x] Integration run downloaded a file from multiple source peers and checked its SHA-256.
- [x] Integration run stopped a source during a download, verified failover and full-file SHA-256, then observed timeout removal and re-replication.
- [ ] Run the complete demo across four actual participant machines and verify LAN addresses/firewall behavior.
- [x] Add the latest successful integration screenshots to [the debugging PDF](evidence/p2p_debug_outputs.pdf), pages 34-37. Earlier failure screenshots remain as troubleshooting history.
- [ ] Record performance measurements and graphs for the report.
- [ ] Finish submission packaging, run instructions, and live demonstration preparation.

## Run peer discovery locally

Open separate PowerShell terminals in the project directory.

Start the first peer:

```powershell
python p2p_file.py 5000
```

Start additional peers:

```powershell
python p2p_file.py 5001 127.0.0.1:5000
python p2p_file.py 5002 127.0.0.1:5000
python p2p_file.py 5003 127.0.0.1:5000
```

These commands run peers on one computer. For peers on separate computers, use the bootstrap computer's reachable LAN address; use `--ip ADDRESS` to override the detected advertised address. The chunk store is separated by peer port.

## Run the one-command integration checker

From the project directory, run:

```powershell
python integration_test.py
```

The checker runs the unit suite, starts four localhost peers, indexes two 2.5 MB files from each, then adds a fifth peer and indexes two files from it. It exercises a lost CHUNK_DATA fragment, BUSY reply, corrupted replica, and source crash during download. The latest run passed the four-peer and late-join checks, including overlapping multi-source download, reconstruction, timeout removal, and replica repair.

Temporary test stores and files are removed when the checker exits. It chooses free consecutive UDP ports unless `P2P_TEST_PORT` is set. The run prints localhost timing measurements and saves them to `output/integration_metrics.csv`. The latest run passed; its timings are local demonstrations, not benchmarks. It does not verify real LAN/firewall behavior or simulate packet loss for every packet type.

In a peer terminal, share a file and inspect the catalog:

```text
/index "C:\\path\\to\\shared-file.bin"
/files
```

Copy the 64-character file ID printed by `/index` or `/files`, then download it on another peer:

```text
/download FILE_ID_HEX
```

For the assignment demo, start at least four peers before indexing files. The code can place chunks with three known peers, but four peers remain the project target. By default, the reconstructed file is saved under `downloads/` using its catalog filename. The source keeps temporary chunks until both assigned peers announce their copies; interrupted replication can leave temporary source chunks behind.

## Workflows

### Peer discovery and membership

Membership control uses binary protocol packets. A newcomer retries JOIN to its configured bootstrap until JOIN_ACK assigns an ID and returns the known peer list. The bootstrap announces it to existing peers with NEW_PEER; heartbeats can also introduce a peer if that announcement was lost. Repeated membership-view mismatches trigger a peer-list request.

```mermaid
flowchart TD
    A[Start peer and bind UDP port] --> B{Bootstrap address provided?}
    B -- No --> C[Start as first peer]
    B -- Yes --> D[Send JOIN to bootstrap]
    D -. No JOIN_ACK after 1 second: resend, at most 5 retries .-> D
    D --> E[Bootstrap records newcomer]
    E --> F[Binary JOIN_ACK carries ID and peer list]
    E --> G[Binary NEW_PEER to existing peers]
    F --> H[Newcomer updates peer list and catalog]
    G --> I[Existing peers add newcomer]
    H --> J[Peers exchange heartbeats]
    I --> J
    J --> K{Membership views match?}
    K -- No, twice --> R[PEER_LIST_REQ and JOIN_ACK repair]
    K -- Yes --> S[Continue membership]
    J --> T{LEAVE received or heartbeat timeout?}
    T -- LEAVE --> L[Remove leaving peer]
    T -- Timeout --> M[Remove silent peer]
```

### Reliable chat

The sender maintains an independent ordered chat stream for each recipient, with up to eight CHAT packets outstanding. Each receiver validates CHAT packets, accepts only the next expected message, ACKs recognized duplicates without displaying them again, and rejects out-of-order packets without an ACK. Per-peer RTO adapts from unambiguous ACK timings.

```mermaid
sequenceDiagram
    participant S as Sending peer
    participant R as Receiving peer
    S->>R: CHAT(session ID, packet sequence, chat sequence, text)
    R->>R: Validate packet and check expected sequence
    alt Expected CHAT
        R->>R: Display once and advance expected counters
        R-->>S: ACK with the exact packet sequence
    else Recognized duplicate
        R-->>S: Send the same ACK again
    else Future or unknown old sequence
        R->>R: Discard without ACK or display
    end
    Note over S,R: If the ACK times out, sender retries the unchanged CHAT
```

The supplied unit suite checks Rule 6/7 cases, and the integration run verified chat delivery without repeated display across the four initial peers and the later joiner. Screenshots of the 17 passing unit tests and successful five-peer integration run are included on pages 34-37 of the evidence PDF.

### File placement and basic download

```mermaid
flowchart LR
    A[Indexer creates placement and chunk SHA-256 hashes] --> B[Send FILE_LIST, CHUNK_HASH, and local CHUNK_HAVE]
    B --> C[Send REPLICATE_REQ to assigned remote owners]
    C --> D[Remote owners request their assigned chunks]
    D --> E[Source sends CHUNK_DATA fragments]
    E --> F[Receiver ACKs fragments and verifies chunk hash]
    F --> F2[Store valid chunk and announce CHUNK_HAVE]
    F2 --> G[Requester fetches chunks in parallel batches of four]
    G --> H[Resume or fail over after three RTOs]
    H --> I[Rebuild file and verify SHA-256]
```

## Current measurements and graphs

The current values below are configuration and local demonstrations, not performance benchmarks.

Heartbeat settings (each `#` represents one second):

```text
Heartbeat interval  ##                  2 seconds
Peer timeout        ######              6 seconds
```

Local CHUNK_DATA reassembly demonstration:

```text
Fragment 1          ##########          500 bytes
Fragment 2          ##########          500 bytes
Reassembled chunk   #################### 1,000 bytes
```

Measured performance graphs still need real test data. Planned graphs include chat delivery latency by peer count, file throughput by file size, and recovery time after a chunk source is stopped.

## Current limitations

- The integration test covers localhost UDP only. Packet-loss simulation, simultaneous joins, peer-list overflow and real multi-machine LAN behavior still need testing.
- The integration test indexes two 2.5 MB files from each of five localhost peers and demonstrates live corrupt-chunk rejection and replica fallback. Larger groups and separate machines still need testing.
- Join-time rebalancing retains older replicas. Safe deletion of surplus copies is not implemented.
- Peer lists, transfer queues, and catalog state are in memory and are rebuilt through peer exchange after restart.
- Performance measurements and graphs are still needed for the report.

## Protocol and evidence documents

- [protocol.md](protocol.md) documents the application-layer packet design.
- [RELIABLE_CHAT_RULES.md](RELIABLE_CHAT_RULES.md) specifies reliable ordered chat behavior.
- [Debugging evidence PDF](evidence/p2p_debug_outputs.pdf) contains terminal screenshots and captions. Add new screenshots after live catalog and file-transfer checks.
