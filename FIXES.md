# Bug Review and Fixes

This document lists the errors found when the project was reviewed against the assignment, and the change made for each one. The review started from commit `03859ca` of `usman-irshad1/p2p`. Every changed place in the code carries a `FIX n` comment with the matching number below.

## Summary

| # | Problem | Severity | Assignment area | Fixed in |
|---:|---|---|---|---|
| 1 | Switching to another replica after a source crash never sent a request | Critical | Fault tolerance | `p2p_file.py` `request_chunk` |
| 2 | A peer that downloaded a file became the owner of every chunk | Critical | Distributed storage | `request_chunk`, `download_file` |
| 3 | The first REPLICATE_REQ for each chunk was sent once, with no retry | Critical | Distributed storage | `distribute_file_chunks`, `retry_initial_replications`, `repair_under_replicated_chunks` |
| 4 | Chunk retransmission did not wait for the RTO | Important | Reliable transfer | `send_chunk_data` |
| 5 | A one-sided peer timeout could block chat or silently drop messages | Important | Group chat | `protocol_design.py`, chat functions in `p2p_file.py` |
| 6 | Peers could not talk across machines; `localhost` as bootstrap failed | Important | Peer discovery | membership handlers, `main` |
| 7 | The code did not follow its own protocol specification | Important | Protocol design | `protocol_design.py`, `send_chunk_data` |
| 8 | Chunk placement could loop forever, crash, or stop rebalancing | Important | Distributed storage | `plan_chunk_owners`, `rebalance_chunks_for_local_peer` |
| 9 | A busy replica was never used, and a failed chunk was never retried | Moderate | Multi-source download | `request_chunk`, `download_file` |
| 10 | Repair always chose the lowest peer ID | Moderate | Re-replication | `repair_under_replicated_chunks` |
| 11 | One error stopped the heartbeat or chat thread for good | Moderate | Membership, chat | `heartbeat`, `chat_sender` |
| 12 | A JOIN_ACK for more than 125 peers was bigger than one datagram | Minor | Membership | `join_ack`, PEER_LIST_REQ reply |
| 13 | "Chat delivery failed" was printed on every peer removal | Minor | Group chat | `clear_pending_chat` |
| 14 | The routing self-test ran on every start and changed the peer list | Minor | Code quality | `main` |
| 15 | All peers on one computer shared one chunk folder | Critical (local demo) | Distributed storage | `main` |
| 16 | Rebalancing could still put a complete file on one peer | Important | Distributed storage | `would_hold_full_file`, rebalance, replication |
| 17 | Windows reported old ICMP errors on the receive socket | Minor | Robustness | `main` |
| 18 | A failed join gave no message | Minor | Usability | `retry_join` |
| 19 | Stale duplicate files and compiled files were in the repository | Cleanup | Submission | repository |

Fixes 15 and 16 were found while testing the other fixes.

## Details

### 1. Failover to another replica never worked (critical)

**Where:** `p2p_file.py`, `request_chunk`, the owner loop.

**Problem:** The download loop gives up on an owner after three RTOs without progress, measured from `download_state["last_progress"]`. When it moved to the next owner, it did not reset that timestamp. For the second owner the check `time.monotonic() - last_progress >= 3 * timeout` was already true, so the loop exited before sending a single CHUNK_REQUEST. When a source crashed during a download, the download always failed with `TimeoutError`, which is the case the assignment's fault-tolerance requirement covers.

**Fix:** Set `last_progress` to the current time whenever a new owner is selected, so each owner gets its own three-RTO budget.

### 2. A downloader became the owner of every chunk (critical)

**Where:** `request_chunk` and `download_file`.

**Problem:** Every chunk fetched by `/download` was written to the shared chunk store and announced with CHUNK_HAVE. After one download, that peer stored every chunk and the catalog listed it as an owner of the whole file. That breaks "no single peer holds a complete file". The rebuilt file in `downloads/` is a separate copy.

**Fix:** `request_chunk` now takes `store_dir` and `announce` arguments. A user download writes its chunks to a private staging folder (`peer_chunks/_download_staging/...`) and does not announce them. Chunks the peer already owns are copied into the staging folder. After the file is assembled and its SHA-256 is checked, the staging folder is deleted. Replication and rebalancing still use the shared store and still announce CHUNK_HAVE.

### 3. Initial replication was not reliable (critical)

**Where:** `distribute_file_chunks`, new `retry_initial_replications`, `repair_under_replicated_chunks`.

**Problem:** When a file was indexed, each REPLICATE_REQ was sent once, with no acknowledgement and no retry. If both requests for a chunk were lost, the catalog listed no owner. The repair code skipped chunks with no owners (`if not owners: continue`), so the only copy was the indexer's unlisted temporary copy, and downloads failed with "No known chunk owners".

**Fix:**
- The indexer keeps each initial REPLICATE_REQ in `pending_initial_replications`. The heartbeat loop resends it every 2 seconds, up to 10 times, until that owner's CHUNK_HAVE arrives or the owner is removed.
- If a chunk has no listed owner left, a peer that still holds a local copy (normally the indexer) now coordinates the repair.

### 4. Chunk retransmission ignored the RTO (important)

**Where:** `send_chunk_data`.

**Problem:** After sending a window, the sender called `event.wait(rto)` once. Every CHUNK_ACK sets that event, and ACKs arrive while the burst is still being sent, so the wait returned almost at once. The next "round" resent every fragment whose ACK was still on its way. All five rounds were used up within milliseconds. This caused heavy unnecessary retransmission. Those fragments were marked as retransmitted, so they gave no RTT samples. Windows were also abandoned early.

**Fix:** Each round now waits until the whole window is acknowledged or one full RTO has passed. The event is cleared and the window is checked under the same lock that the ACK handler uses, so no ACK is missed.

### 5. Chat could block or lose messages after a one-sided timeout (important)

**Where:** `protocol_design.py` (CHAT payload), and `clear_peer_sequence_state`, `classify_incoming_chat`, `outgoing_chat_session`, `chat_sender_once` and the CHAT handler in `p2p_file.py`.

**Problem:** Removing a peer cleared the chat counters in both directions. If only one side timed the other out (for example after a short network pause), the two peers' counters no longer matched:
- If the receiver reset to 1 while the sender continued at N, every message was rejected and that chat stream was blocked for good.
- If the sender restarted at 1 while the receiver still expected N, the new messages matched old entries and were ACKed as duplicates without being displayed. That is silent message loss.

**Fix (protocol version 2):**
- The CHAT payload is now `uint32 session_id | uint32 chat_sequence | UTF-8 text`.
- A sender picks a random nonzero session ID for each recipient stream. When it removes that recipient, it discards the stream; the next stream gets a new session ID and starts at 1.
- A receiver keeps its expected counters per sender and session, even across removal. It resets them only when a new session ID arrives, and it discards datagrams from a session it has already replaced.
- Chat text is now limited to 996 bytes instead of 1,000. `PROTOCOL_VERSION` is now 2, as the spec's versioning rule requires for an incompatible change.

### 6. Peers on different machines could not communicate (important)

**Where:** JOIN_ACK handling in `handle_membership_packet`, new `detect_advertise_ip`, `main`.

**Problem:**
- `advertise_ip` was hard-coded to `127.0.0.1`. The bootstrap put this address in its own JOIN_ACK record. A newcomer on another machine stored the bootstrap at 127.0.0.1, so every packet from the real bootstrap address failed the `known_addr == addr` check and was dropped.
- `localhost:5000` as the bootstrap argument also failed, because the reply comes from `127.0.0.1`, not `localhost`.

**Fix:**
- When a JOIN_ACK arrives, the record for the responding peer uses the UDP source address the reply actually came from.
- The bootstrap host name is resolved with `gethostbyname`.
- A peer advertises the local address it uses to reach the bootstrap. A new `--ip ADDRESS` option overrides this.

### 7. The code did not follow its own specification (important)

**Where:** `protocol_design.py` (`check_datagram`, `make_datagram`, self-test) and `send_chunk_data`.

**Problems:**
- CHUNK_DATA put the fragment number in the header `seq_no`. The spec says `seq_no` must be zero for every packet type except CHAT and ACK.
- The self-test sent FILE_LIST and REPLICATE_REQ with `seq_no` 4 and 5.
- The receiver never checked that `flags` and `group_id` are zero, although the spec requires it.
- Unknown packet types (19 and above) returned `None` instead of being rejected.
- Sequence fields were not checked for packet types 2, 7, 8, 9 and 10.
- The self-test itself failed: it expected the text "1024 bytes", but the error message says "1,024 bytes".

**Fix:**
- CHUNK_DATA now sends `seq_no = 0`, and the self-test uses zero sequence fields.
- `check_datagram` rejects unknown types, nonzero flags or group ID, and nonzero sequence fields on any type except CHAT and ACK (CHAT must also have `ack_no = 0`). `make_datagram` applies the same rules when sending.
- The self-test assertion was corrected.

### 8. Chunk placement could loop, crash or stop rebalancing (important)

**Where:** `plan_chunk_owners`, `rebalance_chunks_for_local_peer`.

**Problems:**
- The loop that moves a chunk away from a peer that owns every chunk could oscillate forever with three peers. It could also raise `StopIteration` when `ranked[2:]` held only the chunk's other owner.
- With three peers and two chunks, no valid placement exists: any two pairs taken from three peers share one peer. The old loop never stopped in that case.
- Rebalancing called `plan_chunk_owners` for every file. A one-chunk file (under 256 KiB) raised `ValueError`, which ended rebalancing for every file listed after it.

**Fix:**
- A move is now accepted only if it strictly reduces the number of peers that would hold the whole file, so the loop always ends. Such a move always exists with at least three peers and three chunks. If no valid placement exists, a clear `ValueError` is raised.
- Rebalancing skips one-chunk files and catches placement errors for one file without stopping the others.

### 9. A busy replica was never tried, and failed chunks were not retried (moderate)

**Where:** `request_chunk`, `download_file`.

**Problem:** An owner already serving two chunks of the download was skipped and never tried. If the only other owner was busy when the first owner failed, the chunk failed at once. `download_file` did not retry failed chunks, so one failed chunk failed the whole download.

**Fix:** A skipped busy owner is queued again at the end and tried with the load limit lifted. A failed chunk is retried up to three times, each time with a fresh owner list from the catalog, which picks up replicas created by repair.

### 10. Repair always chose the lowest peer ID (moderate)

**Where:** `repair_under_replicated_chunks`.

**Problem:** The replacement owner was always `candidates[0]`, the lowest peer ID. After a few departures, that one peer could collect a whole file.

**Fix:** The coordinator picks the candidate that holds the fewest chunks of that file. It never picks a peer that would then hold every chunk.

### 11. One error stopped a background thread (moderate)

**Where:** `heartbeat`, `chat_sender`.

**Problem:** Neither loop had any error handling. One unexpected error ended the thread. A peer that stopped sending heartbeats was then removed by every other peer.

**Fix:** Each loop now calls a `*_once()` function inside `try`/`except`, logs the error and continues. Individual `sendto` failures are ignored.

### 12. JOIN_ACK larger than one datagram (minor)

**Where:** `join_ack` and the PEER_LIST_REQ reply.

**Problem:** A JOIN_ACK with more than 125 peer records is larger than 1,024 bytes, and the receiver rejects it.

**Fix:** The list is cut to 125 records. Peers left out still introduce themselves through NEW_PEER and HEARTBEAT.

### 13. Misleading chat failure message (minor)

**Where:** `clear_pending_chat`.

**Problem:** The test `pending is not None` was always true, because the default value is an empty dictionary. So "Chat delivery … failed" was printed on every peer removal.

**Fix:** The message is printed only when chat was actually queued or waiting for an ACK.

### 14. Self-test ran on every start (minor)

**Where:** `main`.

**Problem:** `test_peer_helpers()` ran at every startup and changed the global peer list.

**Fix:** It now runs only with `--selftest`.

### 15. All peers on one computer shared one chunk folder (critical for the local demo)

**Where:** `main`, `CHUNK_STORE_DIR`.

**Problem:** The chunk store was always `peer_chunks/` next to the script. All peers started from the same folder used the same store, so `has_chunk()` was true on every peer. Distributed storage, replication, the "no complete file" rule and failover were therefore never really exercised in a same-computer demo.

**Fix:** Each peer uses `peer_chunks/<port>/`. The base folder can be changed with the `P2P_STORE_DIR` environment variable, which the integration test uses.

### 16. Rebalancing could still put a complete file on one peer (important)

**Where:** new `would_hold_full_file`, `is_staged_copy`, `local_chunk_indices`; `start_replication_request`, `rebalance_chunks_for_local_peer`, `maybe_release_staged_chunk`.

**Problem:** Join rebalancing keeps older replicas. If two peers briefly had different membership views, the chunks from the two placements could add up to a complete file on one peer. The live test caught this once. Separately, the indexer could announce one of its temporary copies as a replica and later delete it, which left the catalog wrong.

**Fix:**
- A peer refuses a replica or rebalance transfer that would give it every chunk of a file. The check counts stored chunks and transfers in progress.
- Temporary indexer copies are never announced as replicas.
- A copy the peer has announced as its own is never deleted.

### 17. Windows ICMP errors on the receive socket (minor)

**Where:** `main`.

**Problem:** On Windows, an ICMP "port unreachable" from an earlier `sendto` (for example to a crashed peer) is reported as an error on the next `recvfrom`.

**Fix:** This is turned off with `SIO_UDP_CONNRESET` where it is available.

### 18. A failed join gave no message (minor)

**Where:** `retry_join`.

**Fix:** If no JOIN_ACK arrives after all retries, the peer prints a message asking you to check the bootstrap address.

### 19. Repository cleanup

The supplied archive also removed stale scratch files and added an ignore file. In this project update, the ignore file and test files were added, but unrelated existing scratch/duplicate files were left in place.

**Added to this project:**
- `.gitignore` for compiled files, chunk stores and downloads.
- `tests/test_fixes.py`, with unit tests for fixes 5, 7 and 8.
- `tests/integration_test.py`, a live four-peer test.

## Verification

The archive's original review reports running both test suites on Windows with Python 3.14. During this update, I independently ran the archived fixed snapshot on Windows with Python 3.12 before copying its code into this project:

- `python -m unittest discover -s tests -v`: **16 tests passed.** They cover the protocol self-test, the CHAT session payload, the 996-byte text limit, rejection of unknown types, nonzero flags, group ID and sequence fields, and the old version. They also cover both one-sided timeout chat cases and placement for 3 to 5 peers and 2 to 40 chunks. `FailoverTests` checks fix 1 directly: when the first owner stays silent, a CHUNK_REQUEST must still go to the second owner. With fix 1 removed, this test fails, because only the first owner is ever asked. That confirms the original bug.
- `python tests/integration_test.py` starts four real peer processes on local UDP ports. It checks that:
  1. a chat message reaches the other three peers and is shown only once;
  2. two 2.5 MB files are indexed, each into 10 chunks;
  3. every chunk is stored on at least two peers and no peer holds a complete file;
  4. file 1 is downloaded from three source peers, rebuilt, and matches its SHA-256, and the downloader does not become a full-file owner;
  5. when the peer holding the most chunks of file 2 is killed during a download of file 2, the download still completes and its SHA-256 matches;
  6. the survivors detect the crash by heartbeat timeout;
  7. every chunk of file 2 is back on at least two live peers.

  **The archive reports that, with every fix in place, the test passed in 4 of 4 runs.** My independent run also passed. In my run, the killed peer held 9 of the 10 chunks and the download finished 1.2 seconds after it started.

  Two runs made before fix 16 existed also passed. A third run from that time failed: one peer ended up holding all 10 chunks during placement, which is how fix 16 was found.

  The live test also passed in 3 runs with fix 1 removed. Fix 9's per-chunk retry starts a new request, which hides fix 1's bug end to end, so the `FailoverTests` unit test is the check for fix 1.

## Known limitations (not fixed)

- **Peer ID collisions:** two bootstrap peers that accept joins at the same moment can hand out the same random ID. Nothing detects this.
- **Slow recovery from a lost chat message:** the receiver rejects chat messages that arrive early instead of buffering them. One lost message makes the up to seven messages behind it wait for their own timeouts. Ordering is still correct.
- **Sequence numbers do not wrap:** after 65,535 chat messages to one peer, that stream stops. A new session ID could be used to restart it safely, but this is not implemented.
- **Discovery needs a bootstrap address:** there is no LAN broadcast or multicast; the address must be given by hand.
- **Extra replicas are kept:** copies left over from join rebalancing are never deleted.
- **Not yet tested:** behavior under real packet loss, and runs across separate machines. Testing packet loss needs a proxy that drops packets.
