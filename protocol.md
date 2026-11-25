# P2P UDP Group Chat and File Sharing Protocol

**Status:** Version 2 implementation specification. Version 2 adds a 32-bit session ID to CHAT (see FIXES.md, fix 5); every other packet layout is unchanged from version 1. A peer rejects datagrams that carry any other version. A four-peer live run (chat, two 2.5 MB files, multi-source download, a source crash during a download, and re-replication) passes in `tests/integration_test.py`; packet-loss behavior still needs a lossy-network test.

## 1. Purpose and network model

This is an application-layer protocol carried in UDP datagrams. Each peer has a UDP socket and can send to and receive from other peers. A bootstrap peer may help a newcomer discover the group, but it is not a permanent server and does not store the group's files centrally.

The current `p2p_file.py` uses binary protocol datagrams for membership, chat, and file catalog/chunk transfer. The bootstrap peer assigns an ID to a newcomer, but it is only a discovery helper; peers exchange data directly and no central file store is used. File placement, rebalancing, repair, and chunk integrity are implemented but still require live multi-peer verification.

## 2. Datagram layout

Each binary datagram is:

```text
20-byte header | payload bytes
```

The current Python format is:

```python
HEADER_FORMAT = "!HHBBHIHHHH"
```

`!` means network byte order (big-endian) with standard field sizes. The format is 20 bytes long.

| Order | Field | Format | Size | Current/proposed meaning |
|---:|---|---:|---:|---|
| 1 | Source peer ID | `H` | 2 bytes | ID of the sending peer. ID 0 is used only by an unassigned JOIN request. |
| 2 | Destination peer ID | `H` | 2 bytes | ID of the intended peer. ID 0 is reserved for an unassigned JOIN; 65535 is reserved for group broadcast. |
| 3 | Packet type | `B` | 1 byte | Identifies the message format. |
| 4 | Protocol version | `B` | 1 byte | Must be 2. Datagrams with any other value are rejected. |
| 5 | Flags | `H` | 2 bytes | Reserved; all bits must be zero. A receiver rejects nonzero flags. |
| 6 | Group ID | `I` | 4 bytes | Reserved; must be zero (one group per process). A receiver rejects a nonzero group ID. |
| 7 | Sequence number | `H` | 2 bytes | Per-recipient CHAT packet sequence; must be zero for every other packet type (including CHUNK_DATA). |
| 8 | Acknowledgment number | `H` | 2 bytes | Exact CHAT packet sequence carried by ACK; must be zero for every other packet type. |
| 9 | Payload length | `H` | 2 bytes | Number of payload bytes, excluding the 20-byte header. |
| 10 | Checksum | `H` | 2 bytes | 16-bit one's-complement Internet checksum over the full datagram with this field zeroed during calculation. |

### Peer IDs and broadcast

- Peer IDs are 16-bit unsigned values.
- The selected broadcast ID is **65535**. A datagram with destination ID 65535 is intended for all peers.
- `random.randrange(1, 65535)` generates IDs from 1 through 65534, so it does not generate the broadcast ID.
- ID `0` is reserved for an unassigned newcomer. JOIN uses source ID 0 and destination ID 0; the configured UDP address identifies the bootstrap peer. JOIN_ACK assigns the peer ID in its header destination field. Active peer IDs range from 1 through 65534.
- The destination ID is an application-level signal. A peer may still send a separate UDP datagram to every known peer address; that is not the same as using the IP-level UDP broadcast address.

### Length and size rules

- `payload_length` counts bytes, not characters.
- The receiver must reject a datagram when the actual payload length does not equal this field.
- Maximum UDP application datagram: 1,024 bytes. The receiver uses `recvfrom(1024)` and the parser rejects larger messages. A CHUNK_DATA fragment can carry up to 960 data bytes after its 20-byte header and 44-byte fragment metadata.

## 3. Packet type registry

Packet type numbers currently assigned in `protocol_design.py`:

| ID | Name | Payload layout | Current status |
|---:|---|---|---|
| 0 | JOIN | Empty | Live newcomer sends this to the bootstrap UDP address with source and destination IDs set to 0; it retries until JOIN_ACK arrives. |
| 1 | CHAT | `uint32 session_id`, `uint32 chat_sequence`, then UTF-8 text | Live encoder/parser, per-peer ordering, duplicate handling, and exact ACKs exist. |
| 2 | CHUNK_REQUEST | 32-byte file ID + three `uint32` values: `chunk_index`, `start_offset`, `length` | Live handler requests a byte range from a stored chunk; range requests resume partial downloads. |
| 3 | LEAVE | Empty | Live peer sends one unacknowledged packet to each known peer; heartbeat timeout covers a lost LEAVE. |
| 4 | HEARTBEAT | `uint16 peer_count` + `uint32 view_hash` | Sent every 2 seconds to each known peer; peers are removed after 6 seconds without one. |
| 5 | ACK | Empty; acknowledgment number is in header `ack_no` | Live exact-ACK response and matching are used for CHAT. |
| 6 | JOIN_ACK | `uint16 peer_count`, then peer records | Bootstrap assigns the destination ID in the header and returns the active peer snapshot; also used to answer PEER_LIST_REQ. |
| 7 | CHUNK_DATA | 44-byte metadata followed by fragment bytes | Live handler sends/collects fragments and stores a rebuilt chunk. |
| 8 | FILE_LIST | `uint16 count`, then file records | Encoder/parser and live metadata exchange exist. |
| 9 | CHUNK_HAVE | 32-byte file ID + `uint32 chunk_index` | Encoder/parser and live ownership update exist. |
| 10 | REPLICATE_REQ | 32-byte file ID + `uint32 chunk_index` | Encoder/parser exist; receiver asks the sender for its assigned chunk copy. |
| 11 | NEW_PEER | One 8-byte peer record | Bootstrap announces a new member to each existing peer. |
| 12 | CHUNK_NAK | 32-byte file ID + `uint32 chunk_index` + `uint8 reason` | Live handler sends NOT_STORED, BUSY, or HASH_MISMATCH; requester advances to another owner. |
| 13 | CHUNK_ACK | 32-byte file ID + `uint32 chunk_index` + `uint32 cumulative_offset` + `uint32 bitmap` | Live handler acknowledges received fragments and supports selective retransmission in 32-fragment windows. |
| 14 | CHUNK_HASH | 32-byte file ID + `uint32 chunk_index` + 32-byte SHA-256 digest | Live catalog exchange distributes each chunk's expected digest; receivers verify before storing. |
| 15 | CHUNK_HASH_ACK | 32-byte file ID + `uint32 chunk_index` | Acknowledges receipt of the matching CHUNK_HASH; sender retries the same datagram when this ACK is missing. |
| 16 | FILE_LIST_ACK | 32-byte file ID | Acknowledges receipt of one FILE_LIST record; sender retries that record when this ACK is missing. |
| 17 | CHUNK_HAVE_ACK | 32-byte file ID + `uint32 chunk_index` | Acknowledges receipt of a chunk ownership announcement; sender retries it when this ACK is missing. |
| 18 | PEER_LIST_REQ | Empty | Requests a binary JOIN_ACK peer-list snapshot after repeated heartbeat-view mismatches. |

The registry is not complete until every type has a precise purpose, payload layout, and message flow, or is explicitly marked reserved for a later version.

## 4. Payload formats

All multi-byte integers use network byte order. Binary fields are concatenated in the order shown.

### ACK (type 5)

Payload: empty. The sequence being acknowledged is carried in header field `ack_no`.

Version 2 uses exact ACKs for CHAT: `ack_no` equals the `seq_no` of the particular CHAT datagram received. ACK packets have `seq_no = 0`, an empty payload, and are not themselves acknowledged. A receiver repeats the exact ACK when it receives a valid duplicate CHAT, allowing recovery if the original ACK was lost.

### JOIN_ACK (type 6)

Payload:

```text
uint16 peer_count
peer_count repetitions of:
    uint16 peer_id
    4-byte IPv4 address
    uint16 UDP port
```

Each peer record is 8 bytes. `socket.inet_aton(ip)` converts a dotted IPv4 address into its 4-byte representation. The current encoder uses `struct.pack("!H4sH", peer_id, ip_bytes, port)`.

The header source ID is the bootstrap or responding peer. For an initial join, the header destination ID is the newly assigned ID; for peer-list repair it is the requester's existing ID. The payload carries the responder and its currently known peers, excluding the newcomer for the initial response. A peer-list repair response contains the responder's full current snapshot, including the requester. A peer list that exceeds one datagram is not fragmented yet; the 1,024-byte datagram limit allows at most 125 peer records in one JOIN_ACK.

An unassigned newcomer retries the same empty JOIN datagram to its configured bootstrap once per second, for up to five retries after the first send. The bootstrap treats another JOIN from the same UDP source address as a retry and returns the same assigned ID and current peer list.

### PEER_LIST_REQ (type 18)

The payload is empty, and the header `seq_no` and `ack_no` must be zero. A peer sends this request to a known peer after two consecutive heartbeat membership-view mismatches. The receiver replies with JOIN_ACK containing its current active peer snapshot.

### HEARTBEAT (type 4)

Payload:

```text
uint16 peer_count
uint32 view_hash
```

The payload is exactly 6 bytes. The view hash is CRC-32 over the network-byte-order encoding of the sorted active peer IDs, including the sender. Header `seq_no` and `ack_no` are zero. Heartbeats are unacknowledged and sent every 2 seconds; a peer is removed after 6 seconds without one. An otherwise unknown sender may introduce itself with a valid heartbeat, allowing peers to recover a lost NEW_PEER announcement; its address is taken from the UDP source address. Two consecutive peer-count/hash mismatches trigger PEER_LIST_REQ, and the receiver returns JOIN_ACK with its current snapshot.

### CHAT (type 1)

Payload:

```text
uint32 session_id
uint32 chat_sequence
remaining bytes: UTF-8 chat text
```

The header's 16-bit `seq_no` is the reliable packet sequence for one sender-recipient CHAT stream and is matched exactly by the ACK's `ack_no`. The 32-bit `chat_sequence` in the payload orders that sender's chat messages for the same recipient. `session_id` is a nonzero random value the sender chooses when it starts a stream to a recipient; when the sender removes that recipient, it discards the stream and uses a new session ID next time. Within a session, both counters begin at 1 and do not wrap. A receiver keeps its expected counters per sender and session: a new session ID resets them to 1, and datagrams from a session it has already replaced are discarded. With the 8-byte prefix, chat text is limited to 996 UTF-8 bytes. Membership and file packets set both header sequence fields to zero.

### CHUNK_REQUEST and CHUNK_HAVE (types 2 and 9)

CHUNK_REQUEST payload:

```text
32-byte file_id
uint32 chunk_index
uint32 start_offset
uint32 length
```

The CHUNK_REQUEST payload is exactly 44 bytes. `start_offset` is a byte offset in the chunk and must be aligned to the fragment size (960 bytes, except the final partial fragment). `length` is positive and identifies the requested range. A requester resumes at its first missing contiguous byte and can retry another owner after three per-peer RTOs without progress. CHUNK_HAVE continues to use only `file_id` and `chunk_index`.

The current `get_file_id()` helper computes a 32-byte SHA-256 digest of the complete file. This file ID identifies all chunks in its FILE_LIST manifest.

### CHUNK_DATA (type 7)

Payload:

```text
32-byte file_id
uint32 chunk_index
uint32 fragment_offset
uint32 total_chunk_size
remaining bytes: fragment_data
```

The metadata is 44 bytes. `fragment_offset` is the byte offset within the chunk. `total_chunk_size` is the size of the whole chunk and is repeated in every fragment. `fragment_data` contains only the bytes carried by this fragment; there is no padding.

The current live implementation sends fragments of at most 960 bytes, in windows of up to 32 fragments. It paces transmission with a 1 ms pause after each eight-fragment burst and sets the UDP socket receive buffer request to 4 MiB. The receiver validates fragment ranges and the assembled chunk hash before storage. Chunks are kept under `peer_chunks/<port>/<file_id>/<chunk_index>.chunk`, one store per peer process. A user download is staged in a private folder, is never announced with CHUNK_HAVE, and is deleted after the file is assembled, so a downloader does not become an owner of every chunk.

### CHUNK_ACK (type 13) - selective chunk acknowledgement

Payload:

```text
32-byte file_id
uint32 chunk_index
uint32 cumulative_offset
uint32 bitmap
```

The payload is 44 bytes. Header `seq_no` and `ack_no` are both zero; the chunk identity and byte ranges are in the payload. `cumulative_offset` is the first byte offset not yet cumulatively received and is aligned to the 960-byte fragment size, or equals the chunk size when the entire chunk is contiguous. Bitmap bit 0 describes the fragment beginning at `cumulative_offset`; each higher bit describes the next 960-byte fragment. A set bit means that fragment was received. Bit 31 therefore reports the 32nd fragment beginning at or after the cumulative offset. The receiver sends this ACK after each accepted fragment, including a duplicate. The sender keeps a window of at most 32 fragments and retries only missing fragments using the peer's current RTO, for up to five send rounds per window. CHUNK_ACK is not itself acknowledged. The live handler validates the sender, file/chunk identity, and cumulative offset before applying it.

### CHUNK_NAK (type 12) - chunk request failure

Payload:

```text
32-byte file_id
uint32 chunk_index
uint8 reason
```

Reason codes are `1 = NOT_STORED`, `2 = BUSY`, and `3 = HASH_MISMATCH`. Header `seq_no` and `ack_no` are zero. It tells the requester that the selected owner cannot provide this chunk or the data failed integrity checking, so the requester proceeds to another known owner. CHUNK_NAK is not retransmitted. The live handler sends NOT_STORED for unavailable chunks, BUSY when all four outgoing chunk-transfer slots are occupied, and HASH_MISMATCH when a received chunk digest differs from the catalog.

### CHUNK_HASH (type 14) - expected digest for one chunk

Payload:

```text
32-byte file_id
uint32 chunk_index
32-byte chunk_sha256
```

The payload is exactly 68 bytes; header `seq_no` and `ack_no` are zero. Peers send one CHUNK_HASH for each cataloged chunk after FILE_LIST metadata. A receiver may retain a hash that arrives before FILE_LIST and apply it when file metadata arrives. Downloads and replication wait until the expected hash is known. After reassembly, the receiver compares SHA-256 of the chunk bytes with this digest before storing the chunk and announcing CHUNK_HAVE. On mismatch it sends CHUNK_NAK with reason HASH_MISMATCH and does not store the chunk.

### CHUNK_HASH_ACK (type 15) - digest receipt acknowledgment

Payload:

```text
32-byte file_id
uint32 chunk_index
```

The payload is exactly 36 bytes; header `seq_no` and `ack_no` are zero. The receiver sends this ACK after accepting a CHUNK_HASH, including when it receives a duplicate. The sender keeps the original CHUNK_HASH datagram and retries it after one second when no matching ACK arrives, for up to five retries. It clears pending retry state when the ACK matches the sender, file ID, chunk index, and peer address, or when that peer is removed. After the retry limit, it drops the pending state; delivery is not guaranteed beyond that point. CHUNK_HASH_ACK is not itself acknowledged.

### FILE_LIST_ACK (type 16) - file metadata receipt acknowledgment

Payload:

```text
32-byte file_id
```

The payload is exactly 32 bytes; header `seq_no` and `ack_no` are zero. FILE_LIST currently carries one file record per datagram. After validating and merging a record, the receiver returns FILE_LIST_ACK with that record's file ID, including for a duplicate record. The sender retains and retries the original datagram after one second, up to five retries. A matching ACK from the expected peer address clears that pending record; peer removal also clears pending records. An empty FILE_LIST has no file record and therefore needs no per-file ACK. FILE_LIST_ACK itself is not acknowledged.

### CHUNK_HAVE_ACK (type 17) - chunk ownership receipt acknowledgment

Payload:

```text
32-byte file_id
uint32 chunk_index
```

The payload is exactly 36 bytes; header `seq_no` and `ack_no` are zero. After adding the announced owner to its catalog, or retaining the announcement until FILE_LIST arrives, the receiver sends CHUNK_HAVE_ACK. It also ACKs duplicate announcements so a lost ACK can be recovered. The sender retries the original CHUNK_HAVE datagram after one second, up to five retries, and clears pending state when a matching ACK arrives from the expected address or the peer is removed. CHUNK_HAVE_ACK itself is not acknowledged.

### NEW_PEER (type 11)

Payload: one peer record, using the same 8-byte record as JOIN_ACK:

```text
uint16 peer_id | 4-byte IPv4 address | uint16 UDP port
```

The bootstrap sends a separate NEW_PEER datagram to each existing peer after it accepts a newcomer. If a notification is lost, the joining peer's HEARTBEAT can introduce the newcomer to a peer that did not receive it.

### FILE_LIST (type 8)

Payload:

```text
uint16 file_count
file_count repetitions of:
    32-byte file_id (SHA-256 of complete file)
    uint64 file_size
    uint32 chunk_size
    uint16 filename_byte_length
    filename_byte_length bytes of UTF-8 filename
```

The live sender currently sends one record per datagram so each packet fits the 1,024-byte limit. An empty list uses count zero. The metadata catalog is in memory and each record describes fixed-size chunks.

### REPLICATE_REQ (type 10)

Payload is 32-byte `file_id` plus `uint32 chunk_index`. The sender assigns the chunk to the destination peer. The destination then sends CHUNK_REQUEST back to the sender and stores the returned CHUNK_DATA fragments before announcing ownership with CHUNK_HAVE.

## 5. Reliability and ordering

UDP can lose, duplicate, or reorder datagrams. Chat uses the header's 16-bit per-peer sequence and exact acknowledgment fields. Chunk data uses a separate selective-ACK window keyed by file and chunk.

Implemented reliable-chat behavior:

1. A sender assigns a 16-bit sequence number to each CHAT sent to a particular peer. A separate 32-bit chat sequence orders group chat messages from the sender.
2. The receiver checks the sequence number and sends an ACK whose `ack_no` equals that packet's sequence number.
3. The sender maintains an eight-packet per-peer window and retransmits a packet if its ACK does not arrive before its timeout. Per-peer SRTT and RTTVAR update from unambiguous ACK samples; RTO is clamped to 0.2–3 seconds and backs off on retransmission.
4. The receiver ignores duplicate packets for delivery but sends the ACK again.
5. For ordered chat delivery, the receiver accepts only the next expected chat sequence. It discards a future message without ACK; retrying the missing packet allows the receiver to progress.

The per-peer CHAT window is eight datagrams. Per-peer SRTT and RTTVAR are updated from ACKs for packets that were not retransmitted; RTO is clamped to 0.2–3 seconds and doubles on retransmission up to 3 seconds. CHAT packets are retried until acknowledged or that peer is removed. Packet and chat sequence counters start at 1 and do not wrap; when a counter reaches its field limit, the sender must establish a new peer session. ACK packets are not acknowledged.

For chunk transfer, CHUNK_ACK is a separate selective acknowledgement keyed by `(file_id, chunk_index)`. Its cumulative offset and 32-bit fragment bitmap report received byte ranges; it does not use the header's exact `ack_no`. A sender keeps at most 32 fragments outstanding for a chunk and resends only missing fragments using the current peer RTO, for up to five rounds per window. If the requested owner lacks the bytes, it sends CHUNK_NAK with NOT_STORED and the requester tries its next known owner. Source-crash failover was exercised by the four-peer integration test; packet-loss behavior still needs a lossy-network test.

## 6. Message flows

Membership, chat, and file exchange use the binary protocol messages described here.

### Join

1. A newcomer binds its UDP socket and sends an empty binary JOIN with source and destination ID 0 to the configured bootstrap address. It retries the same datagram once per second, up to five retries, until joined.
2. The bootstrap deduplicates a retry by its UDP source address, assigns or reuses an active ID, and replies with JOIN_ACK. The assigned ID is in the JOIN_ACK destination-ID header field; its payload contains the bootstrap and the known peer records.
3. The newcomer records the assigned ID and returned peers. It then sends heartbeats to every known peer and exchanges its file catalog.
4. The bootstrap sends a binary NEW_PEER record separately to each existing peer and sends the newcomer's catalog to it.
5. A peer that misses NEW_PEER can learn the newcomer from its HEARTBEAT source address. Repeated view mismatches are repaired by PEER_LIST_REQ followed by a JOIN_ACK snapshot.

### Heartbeat and failure detection

- Peers send binary HEARTBEAT every 2 seconds to each known peer. It carries active peer count and CRC-32 of the sorted membership IDs; a peer is removed after 6 seconds without a heartbeat.
- After two consecutive view mismatches, a peer sends binary PEER_LIST_REQ; the receiver answers with binary JOIN_ACK containing its current peer snapshot, which the requester merges.
- A peer that exits normally sends one binary LEAVE to each known peer; recipients remove it immediately. If LEAVE is lost, heartbeat timeout removes the peer.
- HEARTBEAT and LEAVE are not transport-ACKed. Heartbeat repetition handles lost heartbeats; the timeout handles a lost LEAVE.

### Chat

Current flow: sender assigns each peer its own sequence stream, sends up to eight outstanding CHAT packets per peer, and retransmits the same datagram on timeout until its exact ACK arrives. The receiver ACKs duplicates but displays each accepted chat sequence once and in order.

Current status: binary CHAT and ACK are used by the live P2P program with per-peer windows, adaptive retransmission timing, exact ACKs, duplicate suppression, and ordered acceptance.

### File transfer

Current basic flow: after at least three peers are known, `/index <path>` hashes a file, divides it into 256 KiB chunks, uses bounded HRW hashing to select two owners per chunk, advertises metadata/ownership, and sends REPLICATE_REQ to remote owners. The indexer keeps each REPLICATE_REQ and resends it every 2 seconds, up to 10 times, until that owner's CHUNK_HAVE arrives or the owner is removed. A peer declines a replica that would give it every chunk of a file. A destination requests its chunk, reassembles CHUNK_DATA, stores it, and announces CHUNK_HAVE. The source removes its temporary chunk copy after both assigned peers report ownership. A requester uses `/download <file_id_hex> [output_path]`; a sliding pool keeps up to four chunk downloads active, prefers chunks with fewer owners, and plans at most two concurrent chunk downloads per source. It sends ranged requests, resumes from the first missing byte, fails over after three per-peer RTOs without progress (each new owner gets its own three-RTO budget), retries a failed chunk up to three times with a fresh owner list, rebuilds the file, and verifies its SHA-256 against the file ID.

Peers exchange one CHUNK_HASH digest per cataloged chunk. For each requested chunk, the source sends CHUNK_DATA in windows of up to 32 fragments. The receiver sends CHUNK_ACK with cumulative progress and a bitmap of later received fragments. After waiting one full peer RTO (or until the whole window is acknowledged), the source retransmits only unacknowledged fragments, for at most five send rounds per window; if a window remains incomplete, the requester can resume with another owner after three RTOs without progress. After reassembly, the receiver checks the chunk digest before storing it. A mismatch produces CHUNK_NAK/HASH_MISMATCH; an unavailable chunk produces CHUNK_NAK/NOT_STORED. In either case, the requester tries its next advertised owner. Source-crash failover was exercised; packet-loss and intentional hash-mismatch behavior still need targeted tests.

After a graceful LEAVE or heartbeat timeout, each peer removes the departed owner from its catalog. If a chunk is under-replicated and a surviving owner has the local chunk bytes, the lowest-ID surviving owner sends `REPLICATE_REQ` to an unassigned active peer. It retries the request periodically until a `CHUNK_HAVE` announcement restores the two-owner count. If no surviving peer has the chunk bytes, this mechanism cannot recover it.

When a peer joins, it uses the current peer IDs and bounded HRW placement algorithm to find chunks assigned to it. Existing owners send `FILE_LIST`, `CHUNK_HASH`, and `CHUNK_HAVE`; the joining peer waits for each expected digest, requests its missing assigned chunks with ranged `CHUNK_REQUEST`, verifies and stores the data, and announces ownership with `CHUNK_HAVE`.

Limitations: initial placement runs when `/index` is issued and needs three known peers and at least two chunks. Join rebalancing adds the joining peer's assigned copies but retains older replicas, so extras can remain. Four-peer placement, multi-source download, source-crash failover, and re-replication passed the supplied localhost integration test. Packet-loss simulation, saturated-source BUSY failover, and live hash-mismatch fallback still need targeted tests; safe removal of surplus replicas is not implemented.

## 7. Current implementation inventory

### Implemented or prototyped

- 20-byte binary header format and packet type mapping through type 18.
- Payload encoders/parsers for CHAT, CHUNK_REQUEST, ACK, JOIN_ACK, PEER_LIST_REQ, CHUNK_DATA, FILE_LIST, FILE_LIST_ACK, CHUNK_HAVE, CHUNK_HAVE_ACK, REPLICATE_REQ, CHUNK_ACK, CHUNK_NAK, CHUNK_HASH, and CHUNK_HASH_ACK.
- FILE_LIST records, CHUNK_HASH digests, and CHUNK_HAVE announcements are acknowledged and retried up to five times when their matching ACK is missing. These recovery paths still need live packet-loss verification.
- File SHA-256 helper and a local two-fragment CHUNK_DATA reassembly demonstration.
- In-memory FILE_LIST/CHUNK_HAVE catalog exchange, REPLICATE_REQ placement, and on-disk chunk store.
- Basic live ranged CHUNK_REQUEST/CHUNK_DATA transfer, an adaptive sliding download pool of up to four chunks, request retries/failover, and whole-file SHA-256 verification.
- Chunk serving is limited to four concurrent outgoing transfers; excess requests receive CHUNK_NAK/BUSY so the requester can try another owner.
- Bounded HRW placement, a requested 4 MiB socket receive buffer, paced CHUNK_DATA bursts, and CRC-32 membership-view repair.
- Binary UDP membership with JOIN retries, ID assignment and peer snapshots in JOIN_ACK, NEW_PEER announcements, HEARTBEAT/LEAVE, heartbeat-view repair, and timeout removal. Four-peer localhost membership and crash timeout were observed by the supplied integration test.
- Local screenshots show three-peer discovery, heartbeat/timeout behavior, and graceful leave behavior.

### Remaining implementation and verification

- Live membership packets use the binary packet layouts documented above. Packet loss, simultaneous join, and peer-list overflow cases still need verification; a JOIN_ACK peer list is currently limited to one datagram.
- Flags and multi-group semantics are not defined for version 2; unused flag bits must stay zero.
- A peer advertises the local address it uses to reach its bootstrap (or the `--ip` value), and a receiver of JOIN_ACK uses the responder's real UDP source address. Runs across separate machines still need network testing.
- Packet-loss simulation and intentional integrity-failure behavior still require targeted live verification. Four-peer behavior, multi-source download, peer failure, and repair passed the supplied localhost integration test.

## 8. Remaining protocol and project checklist

- [x] Document header field semantics, checksum behavior, broadcast ID 65535, unassigned ID 0, exact ACKs, sequence exhaustion, and current retry rules.
- [x] Integrate binary JOIN, JOIN_ACK, NEW_PEER, LEAVE, HEARTBEAT, and PEER_LIST_REQ handlers; four-peer localhost membership was exercised, with packet-loss verification pending.
- [x] Document current JOIN, CHAT, file-transfer, LEAVE, and HEARTBEAT flows.
- [x] Add encoders and parsers for the packet types currently used by chat and file transfer, including ranged CHUNK_REQUEST and PEER_LIST_REQ.
- [x] Validate packet length and checksum before the live handlers accept a packet.
- [ ] Add a round-trip test vector for every packet type and malformed-packet checks.
- [x] Run file placement/download with four peers, multiple sources, and a stopped chunk source; record the integration output and repeat from `python integration_test.py`.
- [ ] Simulate packet loss and intentional chunk corruption; record results.

## 9. Versioning rule

The current protocol version is 2 (version 1 had no CHAT session ID). If peers change the meaning of a field or payload format incompatibly, increment the protocol version or introduce a clearly defined version-negotiation rule. A peer should reject or safely ignore unsupported versions rather than guessing their format.
