import socket
import threading
import queue
import random
import sys
import time
import struct
import os
import hashlib
import shutil
import zlib
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import protocol_design
import file_catalog as file_catalog_helpers
import chunk_store

messages = queue.Queue()
peers = []
last_seen = {}
bootstrap_addr = None
joined_network = False
peer_id = 0
peer_lock = threading.Lock()
sequence_lock = threading.Lock()
chat_state_lock = threading.Lock()
file_catalog_lock = threading.RLock()
chunk_download_lock = threading.Lock()
chunk_send_lock = threading.Lock()
chunk_hash_lock = threading.Lock()
file_list_lock = threading.Lock()
chunk_have_lock = threading.Lock()
rtt_lock = threading.Lock()
outgoing_chat_queues = {}
pending_chats = {}
file_catalog_state = file_catalog_helpers.create_catalog()
file_catalog_seen_at = {}
pending_chunk_have = set()
pending_chunk_hashes = {}
pending_chunk_hash_acks = {}
pending_file_list_acks = {}
pending_chunk_have_acks = {}
peer_rtt_state = {}
peer_view_mismatch_counts = {}
pending_chunk_downloads = {}
pending_chunk_sends = {}
pending_replication_requests = set()
active_replication_tasks = set()
chunk_placement_state = {}
pending_chunk_repairs = {}
chunk_repair_lock = threading.Lock()
# FIX 3: initial REPLICATE_REQ datagrams waiting for the owner's CHUNK_HAVE.
pending_initial_replications = {}
initial_replication_lock = threading.Lock()
local_port = 0
advertise_ip = "127.0.0.1"

PACKET_SEQUENCE_MAX = 0xFFFF
CHAT_SEQUENCE_MAX = 0xFFFFFFFF
next_packet_sequence_by_peer = {}
next_chat_sequence_by_peer = {}
expected_packet_sequence_by_peer = {}
expected_chat_sequence_by_peer = {}
received_chat_pairs_by_peer = {}
# FIX 5: per-direction chat session IDs. A sender picks a fresh random ID
# whenever it (re)starts a stream to a recipient; a receiver resets its
# expected counters only when it sees a different session ID.
outgoing_session_by_peer = {}
receive_session_by_peer = {}
retired_sessions_by_peer = {}
test_preferred_owner_id = None
test_chunk_data_drop_used = False
test_busy_reject_used = False

HEARTBEAT_INTERVAL = 2
PEER_TIMEOUT = 6
JOIN_ACK_TIMEOUT = 1
JOIN_RETRIES = 5
CHAT_WINDOW = 8
INITIAL_RTO = 1.0
MIN_RTO = 0.2
MAX_RTO = 3.0
DEFAULT_CHUNK_SIZE = 256 * 1024
BASE_STORE_DIR = os.environ.get(
    "P2P_STORE_DIR",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "peer_chunks"),
)
CHUNK_STORE_DIR = BASE_STORE_DIR  # replaced by a per-port folder in main()
# FIX 2: user downloads are staged here, never in the shared chunk store.
DOWNLOAD_STAGING_DIR = os.path.join(BASE_STORE_DIR, "_download_staging")
DOWNLOAD_CHUNK_ATTEMPTS = 3
INITIAL_REPLICATION_RETRY_INTERVAL = 2
INITIAL_REPLICATION_RETRIES = 10
CHUNK_FRAGMENT_DATA_SIZE = (
    protocol_design.MAX_DATAGRAM_SIZE - protocol_design.header_size - 44
)
CHUNK_REQUEST_TIMEOUT = 3 * INITIAL_RTO
REPLICATION_STARTUP_GRACE = (
    INITIAL_REPLICATION_RETRY_INTERVAL * (INITIAL_REPLICATION_RETRIES + 1)
    + CHUNK_REQUEST_TIMEOUT
)
CHUNK_FRAGMENT_RETRIES = 8
CHUNK_SEND_WINDOW = 32
CHUNK_SEND_BURST_SIZE = 4
CHUNK_SEND_BURST_DELAY = 0.005
CHUNK_MIN_RTO = 0.5
MAX_ACTIVE_CHUNK_SENDS = 1
active_chunk_send_slots = threading.BoundedSemaphore(MAX_ACTIVE_CHUNK_SENDS)
SOCKET_RECEIVE_BUFFER_SIZE = 4 * 1024 * 1024
CHUNK_HASH_ACK_TIMEOUT = 1
CHUNK_HASH_RETRIES = 5
FILE_LIST_ACK_TIMEOUT = 1
FILE_LIST_RETRIES = 5
CHUNK_HAVE_ACK_TIMEOUT = 1
CHUNK_HAVE_RETRIES = 5
CHUNK_REPAIR_RETRY_INTERVAL = CHUNK_REQUEST_TIMEOUT * 2
MIN_DISTRIBUTION_PEERS = 3
MAX_JOIN_ACK_PEERS = (
    protocol_design.MAX_DATAGRAM_SIZE - protocol_design.header_size - 2
) // 8
MAX_FILE_LIST_FILENAME_BYTES = (
    protocol_design.MAX_DATAGRAM_SIZE
    - protocol_design.header_size
    - struct.calcsize("!H")
    - struct.calcsize("!32sQIH")
)


def peer_snapshot():
    """Return a stable snapshot of the current peer ID/address pairs."""
    with peer_lock:
        return tuple(peers)


def get_peer_rto(known_id):
    """Return the current per-peer retransmission timeout."""
    with rtt_lock:
        state = peer_rtt_state.get(known_id)
        return INITIAL_RTO if state is None else state["rto"]


def update_peer_rtt(known_id, sample):
    """Update SRTT, RTTVAR, and bounded RTO using an unambiguous sample."""
    if sample <= 0:
        return

    with rtt_lock:
        state = peer_rtt_state.get(known_id)
        if state is None:
            srtt = sample
            rttvar = sample / 2
        else:
            srtt = state["srtt"]
            rttvar = 0.75 * state["rttvar"] + 0.25 * abs(srtt - sample)
            srtt = 0.875 * srtt + 0.125 * sample

        peer_rtt_state[known_id] = {
            "srtt": srtt,
            "rttvar": rttvar,
            "rto": min(MAX_RTO, max(MIN_RTO, srtt + 4 * rttvar)),
        }


def peer_view_hash():
    """Hash the sorted IDs in this peer's active membership view."""
    active_ids = sorted({peer_id, *(known_id for known_id, _ in peer_snapshot())})
    packed_ids = b"".join(struct.pack("!H", known_id) for known_id in active_ids)
    return len(active_ids), zlib.crc32(packed_ids) & 0xFFFFFFFF


def send_peer_list_request(destination_id, destination_addr):
    """Ask a peer for its current membership view after a mismatch."""
    datagram = protocol_design.make_datagram(
        packet_name="PEER_LIST_REQ",
        peer_id=peer_id,
        destination_p_id=destination_id,
        protocol_version=protocol_design.PROTOCOL_VERSION,
        flags=0,
        group_id=0,
        seq_no=0,
        payload=None,
    )
    server.sendto(datagram, destination_addr)


def add_peer(peer_id, peer_addr):
    """Add a peer once, keyed by peer ID."""
    with peer_lock:
        if any(known_id == peer_id for known_id, _ in peers):
            return False

        peers.append((peer_id, peer_addr))
        return True


def remove_peer(peer_id, peer_addr=None):
    """Remove a peer and discard its per-direction sequence state."""
    removed = False

    with peer_lock:
        for known_id, known_addr in tuple(peers):
            if known_id == peer_id and (
                peer_addr is None or known_addr == peer_addr
            ):
                peers.remove((known_id, known_addr))
                removed = True
                break

    if removed:
        last_seen.pop(peer_id, None)
        peer_view_mismatch_counts.pop(peer_id, None)
        with rtt_lock:
            peer_rtt_state.pop(peer_id, None)
        clear_peer_sequence_state(peer_id)
        clear_pending_chat(peer_id)
        with file_catalog_lock:
            file_catalog_helpers.remove_peer(file_catalog_state, peer_id)
            pending_chunk_have.difference_update({
                item for item in pending_chunk_have if item[2] == peer_id
            })
        with chunk_repair_lock:
            for repair_key, (target_id, sent_at) in tuple(
                pending_chunk_repairs.items()
            ):
                if target_id == peer_id:
                    pending_chunk_repairs.pop(repair_key, None)
        with chunk_hash_lock:
            for hash_key in tuple(pending_chunk_hash_acks):
                if hash_key[0] == peer_id:
                    pending_chunk_hash_acks.pop(hash_key, None)
        with file_list_lock:
            for file_list_key in tuple(pending_file_list_acks):
                if file_list_key[0] == peer_id:
                    pending_file_list_acks.pop(file_list_key, None)
        with chunk_have_lock:
            for chunk_have_key in tuple(pending_chunk_have_acks):
                if chunk_have_key[0] == peer_id:
                    pending_chunk_have_acks.pop(chunk_have_key, None)

    return removed


def next_packet_sequence(destination_peer_id):
    """Allocate the next 16-bit reliable-packet sequence for this direction."""
    key = (peer_id, destination_peer_id)

    with sequence_lock:
        sequence = next_packet_sequence_by_peer.get(key, 1)
        if sequence > PACKET_SEQUENCE_MAX:
            raise OverflowError(
                "Packet sequence exhausted; rejoin under a new peer ID"
            )
        next_packet_sequence_by_peer[key] = sequence + 1
        return sequence


def next_chat_sequence(destination_peer_id):
    """Allocate the next 32-bit chat sequence for this recipient."""
    key = (peer_id, destination_peer_id)

    with sequence_lock:
        sequence = next_chat_sequence_by_peer.get(key, 1)
        if sequence > CHAT_SEQUENCE_MAX:
            raise OverflowError(
                "Chat sequence exhausted; rejoin under a new peer ID"
            )
        next_chat_sequence_by_peer[key] = sequence + 1
        return sequence


def expected_sequences_from(sender_peer_id):
    """Return this receiver's expected packet and chat numbers from a sender."""
    key = (sender_peer_id, peer_id)

    with sequence_lock:
        packet_sequence = expected_packet_sequence_by_peer.setdefault(key, 1)
        chat_sequence = expected_chat_sequence_by_peer.setdefault(key, 1)
        return packet_sequence, chat_sequence


def advance_expected_sequences_from(sender_peer_id):
    """Advance the two independent expected counters after an accepted CHAT."""
    key = (sender_peer_id, peer_id)

    with sequence_lock:
        packet_sequence = expected_packet_sequence_by_peer.get(key, 1) + 1
        chat_sequence = expected_chat_sequence_by_peer.get(key, 1) + 1
        expected_packet_sequence_by_peer[key] = packet_sequence
        expected_chat_sequence_by_peer[key] = chat_sequence
        return packet_sequence, chat_sequence


def outgoing_chat_session(destination_peer_id):
    """Return this sender's session ID for one recipient, creating it once."""
    key = (peer_id, destination_peer_id)
    with sequence_lock:
        session_id = outgoing_session_by_peer.get(key)
        if session_id is None:
            session_id = random.randint(1, 0xFFFFFFFF)
            outgoing_session_by_peer[key] = session_id
        return session_id


def clear_peer_sequence_state(removed_peer_id):
    """Forget the outgoing stream to a removed peer.

    FIX 5: only the sending direction is reset (with a new session ID next
    time). Receive-side counters are kept and keyed by the sender's session
    ID, so a one-sided timeout can no longer desynchronise the two peers:
    previously the receiver restarted at 1 while the sender continued at N
    (chat blocked forever), or the sender restarted at 1 while the receiver
    expected N (new messages ACKed as "duplicates" and never displayed).
    """
    with sequence_lock:
        for sequence_map in (
            next_packet_sequence_by_peer,
            next_chat_sequence_by_peer,
            outgoing_session_by_peer,
        ):
            sequence_map.pop((peer_id, removed_peer_id), None)


def classify_incoming_chat(sender_peer_id, session_id, packet_sequence, chat_sequence):
    """Accept the expected CHAT, recognize exact retransmits, or reject gaps."""
    key = (sender_peer_id, peer_id)

    with sequence_lock:
        retired_sessions = retired_sessions_by_peer.setdefault(key, set())
        if session_id in retired_sessions:
            # A delayed datagram from a stream the sender already abandoned.
            return "reject"
        current_session = receive_session_by_peer.get(key)
        if current_session != session_id:
            # A new sender stream always starts at sequence 1.
            if current_session is not None:
                retired_sessions.add(current_session)
            receive_session_by_peer[key] = session_id
            expected_packet_sequence_by_peer[key] = 1
            expected_chat_sequence_by_peer[key] = 1
            received_chat_pairs_by_peer[key] = {}
        expected_packet = expected_packet_sequence_by_peer.setdefault(key, 1)
        expected_chat = expected_chat_sequence_by_peer.setdefault(key, 1)
        received_pairs = received_chat_pairs_by_peer.setdefault(key, {})

        if (
            packet_sequence == expected_packet
            and chat_sequence == expected_chat
        ):
            expected_packet_sequence_by_peer[key] = expected_packet + 1
            expected_chat_sequence_by_peer[key] = expected_chat + 1
            received_pairs[packet_sequence] = chat_sequence
            return "accepted"

        if (
            packet_sequence < expected_packet
            and received_pairs.get(packet_sequence) == chat_sequence
        ):
            return "duplicate"

        return "reject"


def send_chat_to_active_peers(chat_datagram):
    """Unicast one chat datagram to each peer in a membership snapshot."""
    recipients = peer_snapshot()

    for known_id, peer_addr in recipients:
        server.sendto(chat_datagram, peer_addr)

    return recipients


def send_file_catalog_to_peer(destination_id, destination_addr):
    """Send known file metadata and this peer's chunk claims to one peer."""
    with file_catalog_lock:
        records = file_catalog_helpers.file_list_records(file_catalog_state)
        chunk_hashes = [
            (file_id, chunk_index, chunk_hash)
            for file_id, metadata in file_catalog_state.items()
            for chunk_index, chunk_hash in metadata["chunk_hashes"].items()
        ]
        local_chunks = [
            (file_id, chunk_index)
            for file_id, metadata in file_catalog_state.items()
            for chunk_index, owners in metadata["chunk_owners"].items()
            if peer_id in owners
        ]

    if not records:
        records_to_send = [()]
    else:
        # FILE_LIST has no fragmentation yet; one record per UDP datagram keeps
        # each message within the current 1,024-byte receive limit.
        records_to_send = [(record,) for record in records]

    for file_records in records_to_send:
        datagram = protocol_design.make_datagram(
            packet_name="FILE_LIST",
            peer_id=peer_id,
            destination_p_id=destination_id,
            protocol_version=protocol_design.PROTOCOL_VERSION,
            flags=0,
            group_id=0,
            seq_no=0,
            payload=file_records,
        )
        if len(datagram) > protocol_design.MAX_DATAGRAM_SIZE:
            raise ValueError("A FILE_LIST record exceeds the 1,024-byte limit")
        if file_records:
            file_id = file_records[0][0]
            key = (destination_id, file_id)
            with file_list_lock:
                pending_file_list_acks[key] = {
                    "addr": destination_addr,
                    "datagram": datagram,
                    "last_sent": time.monotonic(),
                    "retries": 0,
                }
        server.sendto(datagram, destination_addr)

    for file_id, chunk_index, chunk_hash in chunk_hashes:
        send_chunk_hash(destination_id, destination_addr, file_id, chunk_index, chunk_hash)

    for file_id, chunk_index in local_chunks:
        send_chunk_have(destination_id, destination_addr, file_id, chunk_index)


def broadcast_file_catalog():
    """Send this peer's catalog and local chunk claims to every active peer."""
    for destination_id, destination_addr in peer_snapshot():
        send_file_catalog_to_peer(destination_id, destination_addr)


def plan_chunk_owners(file_id, chunk_count, participant_ids):
    """Assign two chunk owners with bounded rendezvous (HRW) hashing."""
    if not isinstance(file_id, bytes) or len(file_id) != 32:
        raise ValueError("file_id must be a 32-byte SHA-256 hash")
    participant_ids = tuple(sorted(set(participant_ids)))
    if len(participant_ids) < 3:
        raise ValueError("At least three peers are required for bounded replication")
    if chunk_count < 2:
        raise ValueError("At least two chunks are required to avoid a full-file copy")

    rankings = {}
    placement = {}
    for chunk_index in range(chunk_count):
        scores = {
            known_id: hashlib.sha256(
                file_id + struct.pack("!IH", chunk_index, known_id)
            ).digest()
            for known_id in participant_ids
        }
        ranked = tuple(
            sorted(
                participant_ids,
                key=lambda known_id: (scores[known_id], -known_id),
                reverse=True,
            )
        )
        rankings[chunk_index] = (ranked, scores)
        placement[chunk_index] = ranked[:2]

    # If HRW puts one peer in every chunk's owner pair, that peer would store
    # the complete file. FIX 8: the old loop moved one assignment at a time
    # and, with three peers, could oscillate forever (or raise StopIteration).
    # Now a move is accepted only if it strictly reduces the number of
    # full-file owners, so the loop always terminates. An improving move
    # always exists for >= 3 peers and >= 3 chunks.
    def full_file_owners(candidate_placement):
        return [
            known_id
            for known_id in participant_ids
            if all(known_id in owners for owners in candidate_placement.values())
        ]

    current_full = full_file_owners(placement)
    while current_full:
        improved = False
        for known_id in current_full:
            # Try X's weakest assignments first, and the best-ranked
            # replacement peers first, to stay close to the HRW result.
            for chunk_index in sorted(
                placement, key=lambda index: rankings[index][1][known_id]
            ):
                ranked, _ = rankings[chunk_index]
                other_owner = next(
                    owner for owner in placement[chunk_index] if owner != known_id
                )
                for replacement in ranked:
                    if replacement in (known_id, other_owner):
                        continue
                    trial = dict(placement)
                    trial[chunk_index] = (other_owner, replacement)
                    trial_full = full_file_owners(trial)
                    if len(trial_full) < len(current_full):
                        placement, current_full = trial, trial_full
                        improved = True
                        break
                if improved:
                    break
            if improved:
                break
        if not improved:
            raise ValueError(
                "Could not place chunks without one peer holding the whole file"
            )

    return placement


def distribute_file_chunks(file_id):
    """Assign two owners, advertise the file, then ask owners to copy chunks."""
    with file_catalog_lock:
        metadata = file_catalog_state.get(file_id)
        if metadata is None:
            raise ValueError("File metadata is not in the catalog")
        chunk_count = metadata["chunk_count"]

    participants = {peer_id}
    participants.update(known_id for known_id, _ in peer_snapshot())
    participants = tuple(sorted(participants))
    placement = {}
    if len(participants) >= MIN_DISTRIBUTION_PEERS and chunk_count >= 2:
        placement = plan_chunk_owners(file_id, chunk_count, participants)

    with file_catalog_lock:
        metadata = file_catalog_state[file_id]
        for chunk_index, owners in metadata["chunk_owners"].items():
            owners.discard(peer_id)
            if placement:
                if peer_id in placement[chunk_index]:
                    owners.add(peer_id)
            else:
                owners.add(peer_id)
        chunk_placement_state[file_id] = placement

    broadcast_file_catalog()

    if not placement:
        return 0

    peer_addresses = dict(peer_snapshot())
    requests_sent = 0
    for chunk_index, owners in placement.items():
        for owner_id in owners:
            if owner_id == peer_id:
                continue
            owner_addr = peer_addresses.get(owner_id)
            if owner_addr is None:
                continue
            request_datagram = protocol_design.make_datagram(
                packet_name="REPLICATE_REQ",
                peer_id=peer_id,
                destination_p_id=owner_id,
                protocol_version=protocol_design.PROTOCOL_VERSION,
                flags=0,
                group_id=0,
                seq_no=0,
                payload=(file_id, chunk_index),
            )
            # FIX 3: keep the request until the owner announces CHUNK_HAVE,
            # and let retry_initial_replications() resend it if it is lost.
            with initial_replication_lock:
                pending_initial_replications[(file_id, chunk_index, owner_id)] = {
                    "addr": owner_addr,
                    "datagram": request_datagram,
                    "last_sent": time.monotonic(),
                    "retries": 0,
                }
            server.sendto(request_datagram, owner_addr)
            requests_sent += 1

    return requests_sent


def retry_initial_replications():
    """Resend initial REPLICATE_REQ datagrams whose owner has not confirmed.

    FIX 3: the original code sent each initial REPLICATE_REQ once. If both
    requests for a chunk were lost, the catalog listed no owner and the
    chunk existed only as an unlisted staged copy on the indexing peer.
    """
    now = time.monotonic()
    active_ids = {known_id for known_id, _ in peer_snapshot()}
    due = []
    with initial_replication_lock:
        for key, state in tuple(pending_initial_replications.items()):
            file_id, chunk_index, owner_id = key
            with file_catalog_lock:
                metadata = file_catalog_state.get(file_id)
                confirmed = (
                    metadata is not None
                    and owner_id in metadata["chunk_owners"].get(chunk_index, ())
                )
            if confirmed or owner_id not in active_ids:
                pending_initial_replications.pop(key, None)
                continue
            if now - state["last_sent"] < INITIAL_REPLICATION_RETRY_INTERVAL:
                continue
            if state["retries"] >= INITIAL_REPLICATION_RETRIES:
                pending_initial_replications.pop(key, None)
                continue
            state["retries"] += 1
            state["last_sent"] = now
            due.append((state["datagram"], state["addr"]))

    for datagram, destination_addr in due:
        try:
            server.sendto(datagram, destination_addr)
        except OSError:
            pass
    return len(due)


def repair_under_replicated_chunks(force=False):
    """Ask a surviving chunk owner to create a replacement replica."""
    known_peer_ids = {known_id for known_id, _ in peer_snapshot()}
    known_peer_ids.add(peer_id)
    now = time.monotonic()

    with file_catalog_lock:
        under_replicated = file_catalog_helpers.under_replicated_chunks(
            file_catalog_state,
            minimum_replicas=2,
        )

    under_replicated_keys = {
        (file_id, chunk_index)
        for file_id, chunk_index, owners in under_replicated
    }
    with chunk_repair_lock:
        for repair_key, (target_id, sent_at) in tuple(
            pending_chunk_repairs.items()
        ):
            if repair_key not in under_replicated_keys or target_id not in known_peer_ids:
                pending_chunk_repairs.pop(repair_key, None)

    peer_addresses = dict(peer_snapshot())
    requests_sent = 0

    for file_id, chunk_index, owner_ids in under_replicated:
        if (
            not force
            and now - file_catalog_seen_at.get(file_id, now)
            < REPLICATION_STARTUP_GRACE
        ):
            continue
        owners = set(owner_ids)

        # Every peer sees the departure; the lowest surviving owner coordinates
        # the repair so the group does not start duplicate transfers.
        # FIX 3: when no listed owner is left, a peer that still holds a
        # staged (unlisted) copy, normally the indexer, coordinates instead.
        has_local_copy = chunk_store.has_chunk(CHUNK_STORE_DIR, file_id, chunk_index)
        if owners:
            if peer_id != min(owners):
                continue
        elif not has_local_copy:
            continue
        if not has_local_copy:
            continue

        # FIX 10: prefer the candidate holding the fewest chunks of this file
        # and never one that would then hold every chunk, instead of always
        # picking the lowest peer ID (which concentrated whole files).
        with file_catalog_lock:
            file_metadata = file_catalog_state.get(file_id)
            chunk_total = file_metadata["chunk_count"] if file_metadata else 0
            held_counts = {}
            if file_metadata is not None:
                for other_owners in file_metadata["chunk_owners"].values():
                    for owner in other_owners:
                        held_counts[owner] = held_counts.get(owner, 0) + 1
        candidates = sorted(
            (
                candidate
                for candidate in known_peer_ids - owners - {peer_id}
                if held_counts.get(candidate, 0) + 1 < chunk_total
            ),
            key=lambda candidate: (held_counts.get(candidate, 0), candidate),
        )
        if not candidates:
            continue

        repair_key = (file_id, chunk_index)
        with chunk_repair_lock:
            pending = pending_chunk_repairs.get(repair_key)
            if pending is not None:
                target_id, sent_at = pending
                if target_id in candidates and now - sent_at < CHUNK_REPAIR_RETRY_INTERVAL:
                    continue
                if target_id not in candidates:
                    target_id = candidates[0]
            else:
                target_id = candidates[0]
            pending_chunk_repairs[repair_key] = (target_id, now)

        target_addr = peer_addresses.get(target_id)
        if target_addr is None:
            with chunk_repair_lock:
                pending_chunk_repairs.pop(repair_key, None)
            continue

        try:
            datagram = protocol_design.make_datagram(
                packet_name="REPLICATE_REQ",
                peer_id=peer_id,
                destination_p_id=target_id,
                protocol_version=protocol_design.PROTOCOL_VERSION,
                flags=0,
                group_id=0,
                seq_no=0,
                payload=(file_id, chunk_index),
            )
            server.sendto(datagram, target_addr)
            requests_sent += 1
            print(
                f"Requested replacement replica for chunk {chunk_index} "
                f"of {file_id.hex()[:8]} from peer {target_id}"
            )
        except OSError:
            with chunk_repair_lock:
                pending_chunk_repairs.pop(repair_key, None)

    return requests_sent


def rebalance_chunk_from_peers(file_id, chunk_index):
    """Fetch one chunk assigned to this peer by the current placement plan."""
    task_key = (file_id, chunk_index, peer_id)
    try:
        request_chunk(file_id, chunk_index)
        print(
            f"Rebalanced chunk {chunk_index} of {file_id.hex()[:8]} "
            f"onto peer {peer_id}"
        )
    except (OSError, ValueError, TimeoutError) as error:
        print(
            f"Could not rebalance chunk {chunk_index} of "
            f"{file_id.hex()[:8]}: {error}"
        )
    finally:
        with chunk_download_lock:
            active_replication_tasks.discard(task_key)


def rebalance_chunks_for_local_peer():
    """Fetch missing chunks assigned to this peer when it joins the network.

    Existing replicas are kept; rebalancing adds the new placement's copies
    without deleting any data from current owners.
    """
    if not joined_network:
        return 0

    participants = {peer_id}
    participants.update(known_id for known_id, _ in peer_snapshot())
    if len(participants) < MIN_DISTRIBUTION_PEERS:
        return 0

    with file_catalog_lock:
        file_entries = tuple(
            (
                file_id,
                metadata["chunk_count"],
                set(metadata["chunk_hashes"]),
                {
                    chunk_index: set(owners)
                    for chunk_index, owners in metadata["chunk_owners"].items()
                },
            )
            for file_id, metadata in file_catalog_state.items()
        )

    tasks_started = 0
    for file_id, chunk_count, known_hash_indices, chunk_owners in file_entries:
        # FIX 8: a one-chunk file has no distributed placement. Calling
        # plan_chunk_owners for it raised ValueError and aborted rebalancing
        # for every file listed after it in the catalog.
        if chunk_count < 2:
            continue
        try:
            placement = plan_chunk_owners(file_id, chunk_count, participants)
        except (ValueError, StopIteration) as error:
            print(f"Skipping rebalance of {file_id.hex()[:8]}: {error}")
            continue
        for chunk_index, designated_owners in placement.items():
            if peer_id not in designated_owners:
                continue
            if chunk_index not in known_hash_indices:
                continue
            if peer_id in chunk_owners[chunk_index]:
                continue

            if chunk_store.has_chunk(CHUNK_STORE_DIR, file_id, chunk_index):
                # FIX 16: a temporary indexer copy is deleted later, so it
                # must not be announced as a permanent replica.
                if not is_staged_copy(file_id, chunk_index):
                    announce_chunk_have(file_id, chunk_index)
                continue
            if not chunk_owners[chunk_index]:
                continue
            if would_hold_full_file(file_id, chunk_index):
                continue

            task_key = (file_id, chunk_index, peer_id)
            with chunk_download_lock:
                if any(
                    active_file_id == file_id
                    and active_chunk_index == chunk_index
                    for active_file_id, active_chunk_index, _
                    in active_replication_tasks
                ):
                    continue
                active_replication_tasks.add(task_key)

            threading.Thread(
                target=rebalance_chunk_from_peers,
                args=(file_id, chunk_index),
                daemon=True,
            ).start()
            tasks_started += 1

    return tasks_started


def register_file_metadata(file_path):
    """Index a file, save its chunks locally, and announce their availability."""
    filename = os.path.basename(os.fspath(file_path))
    if len(filename.encode("utf-8")) > MAX_FILE_LIST_FILENAME_BYTES:
        raise ValueError(
            "Filename is too long for the current FILE_LIST datagram limit"
        )

    with file_catalog_lock:
        file_id = file_catalog_helpers.register_file(
            file_catalog_state,
            file_path,
            DEFAULT_CHUNK_SIZE,
        )
        file_catalog_seen_at.setdefault(file_id, time.monotonic())
        metadata = file_catalog_state[file_id].copy()

    chunk_indices = chunk_store.split_and_store_file(
        file_path,
        CHUNK_STORE_DIR,
        file_id,
        metadata["chunk_size"],
    )
    with file_catalog_lock:
        for chunk_index in chunk_indices:
            chunk_data = chunk_store.load_chunk(
                CHUNK_STORE_DIR,
                file_id,
                chunk_index,
            )
            file_catalog_helpers.set_chunk_hash(
                file_catalog_state,
                file_id,
                chunk_index,
                hashlib.sha256(chunk_data).digest(),
            )
    requests_sent = distribute_file_chunks(file_id)
    return file_id, metadata, len(chunk_indices), requests_sent


def send_chunk_ack(destination_id, destination_addr, file_id, chunk_index, fragments):
    """Acknowledge received fragments with a cumulative offset and 32-bit bitmap."""
    received_offsets = set(fragments)
    cumulative_offset = 0
    while cumulative_offset in received_offsets:
        cumulative_offset += len(fragments[cumulative_offset])

    bitmap = 0
    for bit_index in range(CHUNK_SEND_WINDOW):
        fragment_offset = cumulative_offset + bit_index * CHUNK_FRAGMENT_DATA_SIZE
        if fragment_offset in received_offsets:
            bitmap |= 1 << bit_index

    datagram = protocol_design.make_datagram(
        packet_name="CHUNK_ACK",
        peer_id=peer_id,
        destination_p_id=destination_id,
        protocol_version=protocol_design.PROTOCOL_VERSION,
        flags=0,
        group_id=0,
        seq_no=0,
        payload=(file_id, chunk_index, cumulative_offset, bitmap),
    )
    server.sendto(datagram, destination_addr)


def send_chunk_nak(destination_id, destination_addr, file_id, chunk_index, reason):
    """Tell a requester why this peer cannot serve its requested chunk."""
    datagram = protocol_design.make_datagram(
        packet_name="CHUNK_NAK",
        peer_id=peer_id,
        destination_p_id=destination_id,
        protocol_version=protocol_design.PROTOCOL_VERSION,
        flags=0,
        group_id=0,
        seq_no=0,
        payload=(file_id, chunk_index, reason),
    )
    server.sendto(datagram, destination_addr)


def send_chunk_hash(destination_id, destination_addr, file_id, chunk_index, chunk_hash):
    """Send one expected digest and retain it until the receiver acknowledges."""
    datagram = protocol_design.make_datagram(
        packet_name="CHUNK_HASH",
        peer_id=peer_id,
        destination_p_id=destination_id,
        protocol_version=protocol_design.PROTOCOL_VERSION,
        flags=0,
        group_id=0,
        seq_no=0,
        payload=(file_id, chunk_index, chunk_hash),
    )
    key = (destination_id, file_id, chunk_index)
    with chunk_hash_lock:
        pending_chunk_hash_acks[key] = {
            "addr": destination_addr,
            "datagram": datagram,
            "last_sent": time.monotonic(),
            "retries": 0,
        }
    server.sendto(datagram, destination_addr)


def send_chunk_hash_ack(destination_id, destination_addr, file_id, chunk_index):
    """Acknowledge a received digest, including a duplicate CHUNK_HASH."""
    datagram = protocol_design.make_datagram(
        packet_name="CHUNK_HASH_ACK",
        peer_id=peer_id,
        destination_p_id=destination_id,
        protocol_version=protocol_design.PROTOCOL_VERSION,
        flags=0,
        group_id=0,
        seq_no=0,
        payload=(file_id, chunk_index),
    )
    server.sendto(datagram, destination_addr)


def send_file_list_ack(destination_id, destination_addr, file_id):
    """Acknowledge receipt of one file's catalog metadata."""
    datagram = protocol_design.make_datagram(
        packet_name="FILE_LIST_ACK",
        peer_id=peer_id,
        destination_p_id=destination_id,
        protocol_version=protocol_design.PROTOCOL_VERSION,
        flags=0,
        group_id=0,
        seq_no=0,
        payload=file_id,
    )
    server.sendto(datagram, destination_addr)


def send_chunk_have(destination_id, destination_addr, file_id, chunk_index):
    """Announce locally stored chunk ownership and retain it until ACKed."""
    datagram = protocol_design.make_datagram(
        packet_name="CHUNK_HAVE",
        peer_id=peer_id,
        destination_p_id=destination_id,
        protocol_version=protocol_design.PROTOCOL_VERSION,
        flags=0,
        group_id=0,
        seq_no=0,
        payload=(file_id, chunk_index),
    )
    key = (destination_id, file_id, chunk_index)
    with chunk_have_lock:
        pending_chunk_have_acks[key] = {
            "addr": destination_addr,
            "datagram": datagram,
            "last_sent": time.monotonic(),
            "retries": 0,
        }
    server.sendto(datagram, destination_addr)


def send_chunk_have_ack(destination_id, destination_addr, file_id, chunk_index):
    """Acknowledge a received chunk-ownership announcement."""
    datagram = protocol_design.make_datagram(
        packet_name="CHUNK_HAVE_ACK",
        peer_id=peer_id,
        destination_p_id=destination_id,
        protocol_version=protocol_design.PROTOCOL_VERSION,
        flags=0,
        group_id=0,
        seq_no=0,
        payload=(file_id, chunk_index),
    )
    server.sendto(datagram, destination_addr)


def retry_chunk_hashes():
    """Retry unacknowledged chunk hashes five times, then drop pending state."""
    while True:
        now = time.monotonic()
        due_datagrams = []
        with chunk_hash_lock:
            for key, state in tuple(pending_chunk_hash_acks.items()):
                if now - state["last_sent"] < CHUNK_HASH_ACK_TIMEOUT:
                    continue
                if state["retries"] >= CHUNK_HASH_RETRIES:
                    pending_chunk_hash_acks.pop(key, None)
                    continue

                state["retries"] += 1
                state["last_sent"] = now
                due_datagrams.append((state["datagram"], state["addr"]))

        for datagram, destination_addr in due_datagrams:
            try:
                server.sendto(datagram, destination_addr)
            except OSError:
                pass

        time.sleep(CHUNK_HASH_ACK_TIMEOUT / 2)


def retry_file_lists():
    """Retry unacknowledged FILE_LIST records up to five times."""
    while True:
        now = time.monotonic()
        due_datagrams = []
        with file_list_lock:
            for key, state in tuple(pending_file_list_acks.items()):
                if now - state["last_sent"] < FILE_LIST_ACK_TIMEOUT:
                    continue
                if state["retries"] >= FILE_LIST_RETRIES:
                    pending_file_list_acks.pop(key, None)
                    continue

                state["retries"] += 1
                state["last_sent"] = now
                due_datagrams.append((state["datagram"], state["addr"]))

        for datagram, destination_addr in due_datagrams:
            try:
                server.sendto(datagram, destination_addr)
            except OSError:
                pass

        time.sleep(FILE_LIST_ACK_TIMEOUT / 2)


def retry_chunk_have():
    """Retry unacknowledged CHUNK_HAVE announcements up to five times."""
    while True:
        now = time.monotonic()
        due_datagrams = []
        with chunk_have_lock:
            for key, state in tuple(pending_chunk_have_acks.items()):
                if now - state["last_sent"] < CHUNK_HAVE_ACK_TIMEOUT:
                    continue
                if state["retries"] >= CHUNK_HAVE_RETRIES:
                    pending_chunk_have_acks.pop(key, None)
                    continue

                state["retries"] += 1
                state["last_sent"] = now
                due_datagrams.append((state["datagram"], state["addr"]))

        for datagram, destination_addr in due_datagrams:
            try:
                server.sendto(datagram, destination_addr)
            except OSError:
                pass

        time.sleep(CHUNK_HAVE_ACK_TIMEOUT / 2)


def send_chunk_data(
    destination_id,
    destination_addr,
    file_id,
    chunk_index,
    start_offset=0,
    length=None,
):
    """Send a chunk in 32-fragment windows and retry fragments lacking ACKs."""
    try:
        chunk_data = chunk_store.load_chunk(
            CHUNK_STORE_DIR,
            file_id,
            chunk_index,
        )
    except (OSError, KeyError):
        return
    total_chunk_size = len(chunk_data)
    if length is None:
        length = total_chunk_size - start_offset
    end_offset = start_offset + length
    if (
        start_offset < 0
        or start_offset % CHUNK_FRAGMENT_DATA_SIZE != 0
        or length <= 0
        or end_offset > total_chunk_size
    ):
        return
    fragments = []
    for fragment_offset in range(start_offset, end_offset, CHUNK_FRAGMENT_DATA_SIZE):
        fragment_data = chunk_data[
            fragment_offset:fragment_offset + CHUNK_FRAGMENT_DATA_SIZE
        ]
        datagram = protocol_design.make_datagram(
            packet_name="CHUNK_DATA",
            peer_id=peer_id,
            destination_p_id=destination_id,
            protocol_version=protocol_design.PROTOCOL_VERSION,
            flags=0,
            group_id=0,
            # FIX 7: the spec requires seq_no = 0 for every non-CHAT packet;
            # the fragment position is already carried in fragment_offset.
            seq_no=0,
            payload=(file_id, chunk_index, fragment_offset, total_chunk_size, fragment_data),
        )
        fragments.append((fragment_offset, datagram))

    transfer_key = (destination_id, file_id, chunk_index)
    send_state = {
        "addr": destination_addr,
        "total_size": total_chunk_size,
        "fragments": fragments,
        "acked": set(),
        "first_sent": {},
        "retransmitted": set(),
        "error": None,
        "event": threading.Event(),
    }
    with chunk_send_lock:
        if transfer_key in pending_chunk_sends:
            return
        pending_chunk_sends[transfer_key] = send_state

    try:
        fragment_count = len(fragments)
        for window_start in range(0, fragment_count, CHUNK_SEND_WINDOW):
            window_end = min(window_start + CHUNK_SEND_WINDOW, fragment_count)
            window_offsets = {
                fragments[index][0] for index in range(window_start, window_end)
            }

            for _ in range(CHUNK_FRAGMENT_RETRIES):
                with chunk_send_lock:
                    if send_state["error"] is not None:
                        return
                    missing_offsets = window_offsets - send_state["acked"]
                    send_state["event"].clear()
                if not missing_offsets:
                    break

                sent_in_burst = 0
                for fragment_offset, datagram in fragments[window_start:window_end]:
                    if fragment_offset in missing_offsets:
                        with chunk_send_lock:
                            if fragment_offset in send_state["first_sent"]:
                                send_state["retransmitted"].add(fragment_offset)
                                if os.environ.get("P2P_TEST_TRACE") == "1":
                                    print(
                                        f"P2P_TEST_CHUNK_RETRANSMIT:{destination_id}:"
                                        f"{file_id.hex()}:{chunk_index}:{fragment_offset}",
                                        flush=True,
                                    )
                            else:
                                send_state["first_sent"][fragment_offset] = time.monotonic()
                        global test_chunk_data_drop_used
                        if (
                            os.environ.get("P2P_TEST_DROP_FIRST_CHUNK_DATA") == "1"
                            and not test_chunk_data_drop_used
                        ):
                            test_chunk_data_drop_used = True
                            print(
                                f"P2P_TEST_DROPPED_CHUNK_DATA:{destination_id}:"
                                f"{file_id.hex()}:{chunk_index}:{fragment_offset}",
                                flush=True,
                            )
                            continue
                        server.sendto(datagram, destination_addr)
                        sent_in_burst += 1
                        if sent_in_burst >= CHUNK_SEND_BURST_SIZE:
                            time.sleep(CHUNK_SEND_BURST_DELAY)
                            sent_in_burst = 0

                # FIX 4: wait until the whole window is acknowledged or one
                # full RTO has passed. The old single event.wait() returned
                # on the first CHUNK_ACK, so the next "round" resent every
                # fragment whose ACK was still in flight and burned all five
                # rounds in a few milliseconds.
                deadline = time.monotonic() + max(
                    CHUNK_MIN_RTO,
                    get_peer_rto(destination_id),
                )
                while True:
                    with chunk_send_lock:
                        if send_state["error"] is not None:
                            return
                        if window_offsets.issubset(send_state["acked"]):
                            break
                        send_state["event"].clear()
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    send_state["event"].wait(remaining)

            with chunk_send_lock:
                window_complete = window_offsets.issubset(send_state["acked"])
            if not window_complete:
                return
    except OSError as error:
        print(f"Chunk send to peer {destination_id} failed: {error}")
    finally:
        with chunk_send_lock:
            if pending_chunk_sends.get(transfer_key) is send_state:
                pending_chunk_sends.pop(transfer_key, None)
        if os.environ.get("P2P_TEST_TRACE") == "1":
            print(
                f"P2P_TEST_SEND_RESULT:{destination_id}:{file_id.hex()}:"
                f"{chunk_index}:{len(send_state['acked'])}/{len(fragments)}",
                flush=True,
            )


def send_chunk_data_with_slot(
    destination_id,
    destination_addr,
    file_id,
    chunk_index,
    start_offset,
    length,
):
    """Run one reserved upload slot and release it on every exit path."""
    try:
        send_chunk_data(
            destination_id,
            destination_addr,
            file_id,
            chunk_index,
            start_offset,
            length,
        )
    finally:
        active_chunk_send_slots.release()


def contiguous_chunk_offset(fragments):
    """Return the first byte offset not present in a fragment map."""
    offset = 0
    while offset in fragments:
        offset += len(fragments[offset])
    return offset


def request_chunk(
    file_id,
    chunk_index,
    preferred_owner_id=None,
    preferred_addr=None,
    owner_order=None,
    source_loads=None,
    source_load_lock=None,
    source_reservation=None,
    store_dir=None,
    announce=True,
):
    """Request a chunk, resume at the first missing byte, and fail over quickly.

    ``store_dir`` selects where the verified chunk is written. Replica and
    rebalance transfers use the shared chunk store and announce CHUNK_HAVE;
    a user download (FIX 2) writes to a private staging folder and passes
    ``announce=False`` so the downloader never becomes a full-file owner.
    """
    if store_dir is None:
        store_dir = CHUNK_STORE_DIR
    with file_catalog_lock:
        metadata = file_catalog_state.get(file_id)
        if metadata is None:
            raise ValueError("File metadata is not in the catalog")
        if chunk_index not in metadata["chunk_owners"]:
            raise ValueError("Chunk index is outside the file")
        owners = tuple(sorted(metadata["chunk_owners"][chunk_index]))
        file_size = metadata["file_size"]
        chunk_size = metadata["chunk_size"]
        expected_chunk_hash = metadata["chunk_hashes"].get(chunk_index)

    if expected_chunk_hash is None:
        raise ValueError("Expected chunk hash has not arrived from the catalog")

    if chunk_store.has_chunk(store_dir, file_id, chunk_index):
        if preferred_owner_id is not None and announce:
            announce_chunk_have(file_id, chunk_index)
        return True

    expected_size = min(
        chunk_size,
        max(0, file_size - chunk_index * chunk_size),
    )
    if expected_size == 0:
        raise ValueError("The requested chunk has no data")

    peer_addresses = dict(peer_snapshot())
    catalog_owners = owners
    if preferred_owner_id is not None:
        owners = (preferred_owner_id,)
        if peer_addresses.get(preferred_owner_id) != preferred_addr:
            raise ValueError("Replication request came from an unknown peer")
    elif owner_order is not None:
        owners = tuple(owner_order) + tuple(
            owner_id for owner_id in catalog_owners if owner_id not in owner_order
        )
    elif owners:
        start = chunk_index % len(owners)
        owners = owners[start:] + owners[:start]
    key = (file_id, chunk_index)
    download_state = {
        "owner_id": None,
        "owner_addr": None,
        "expected_size": expected_size,
        "expected_hash": expected_chunk_hash,
        "total_size": expected_size,
        "fragments": {},
        "event": threading.Event(),
        "error": None,
        "error_reason": None,
        "last_progress": time.monotonic(),
        "store_dir": store_dir,
    }

    with chunk_download_lock:
        if key in pending_chunk_downloads:
            raise ValueError("This chunk is already being downloaded")
        pending_chunk_downloads[key] = download_state

    # FIX 9: an owner skipped because it already serves two chunks is
    # queued again at the end with the load cap lifted, so a busy replica is
    # still used as the last resort instead of failing the chunk outright.
    attempt_queue = [(owner_id, True) for owner_id in owners]
    attempt_index = 0
    try:
        while attempt_index < len(attempt_queue):
            owner_id, respect_load_cap = attempt_queue[attempt_index]
            attempt_index += 1
            if owner_id == peer_id or owner_id not in peer_addresses:
                continue

            owner_addr = peer_addresses[owner_id]
            if source_loads is not None and source_load_lock is not None:
                with source_load_lock:
                    reserved_owner_id = (
                        source_reservation.get("owner_id")
                        if source_reservation is not None
                        else None
                    )
                    if reserved_owner_id != owner_id:
                        if respect_load_cap and source_loads.get(owner_id, 0) >= 2:
                            attempt_queue.append((owner_id, False))
                            continue
                        if reserved_owner_id is not None:
                            source_loads[reserved_owner_id] = max(
                                0,
                                source_loads.get(reserved_owner_id, 1) - 1,
                            )
                        source_loads[owner_id] = source_loads.get(owner_id, 0) + 1
                        if source_reservation is not None:
                            source_reservation["owner_id"] = owner_id
            timeout = get_peer_rto(owner_id)
            progress_timeout = max(
                CHUNK_REQUEST_TIMEOUT,
                CHUNK_FRAGMENT_RETRIES * max(CHUNK_MIN_RTO, timeout)
                + CHUNK_REQUEST_TIMEOUT,
            )
            with chunk_download_lock:
                download_state["owner_id"] = owner_id
                download_state["owner_addr"] = owner_addr
                download_state["error"] = None
                download_state["error_reason"] = None
                # FIX 1: give every new owner its own no-progress budget.
                # Without this reset, the previous owner's stale timestamp
                # made the loop give up on the replica before asking it.
                download_state["last_progress"] = time.monotonic()
                download_state["event"].clear()

            while True:
                with chunk_download_lock:
                    start_offset = contiguous_chunk_offset(
                        download_state["fragments"]
                    )
                    if start_offset >= expected_size:
                        download_state["event"].set()
                    last_progress = download_state["last_progress"]
                    current_error = download_state["error"]

                if download_state["event"].is_set():
                    if current_error is None:
                        if announce:
                            announce_chunk_have(file_id, chunk_index)
                        return (
                            source_reservation.get("owner_id", owner_id)
                            if source_reservation is not None
                            else owner_id
                        )
                    if download_state["error_reason"] == protocol_design.CHUNK_NAK_REASONS["HASH_MISMATCH"]:
                        with chunk_download_lock:
                            download_state["fragments"].clear()
                            download_state["last_progress"] = time.monotonic()
                    break

                if time.monotonic() - last_progress >= progress_timeout:
                    break

                request_datagram = protocol_design.make_datagram(
                    packet_name="CHUNK_REQUEST",
                    peer_id=peer_id,
                    destination_p_id=owner_id,
                    protocol_version=protocol_design.PROTOCOL_VERSION,
                    flags=0,
                    group_id=0,
                    seq_no=0,
                    payload=(
                        file_id,
                        chunk_index,
                        start_offset,
                        expected_size - start_offset,
                    ),
                )
                server.sendto(request_datagram, owner_addr)
                download_state["event"].wait(timeout)

    finally:
        with chunk_download_lock:
            pending_chunk_downloads.pop(key, None)

    raise TimeoutError(
        f"No peer delivered chunk {chunk_index} of {file_id.hex()[:8]}"
    )


def replicate_chunk_from_peer(file_id, chunk_index, owner_id, owner_addr):
    """Fetch one assigned replica from the peer that requested replication."""
    task_key = (file_id, chunk_index, owner_id)
    try:
        request_chunk(file_id, chunk_index, owner_id, owner_addr)
    except (OSError, ValueError, TimeoutError) as error:
        print(f"Chunk replica {chunk_index} failed: {error}")
    finally:
        with chunk_download_lock:
            active_replication_tasks.discard(task_key)


def local_chunk_indices(file_id):
    """Return the chunk indices of one file stored in this peer's chunk store."""
    folder = os.path.join(CHUNK_STORE_DIR, file_id.hex())
    if not os.path.isdir(folder):
        return set()
    indices = set()
    for name in os.listdir(folder):
        if name.endswith(".chunk"):
            try:
                indices.add(int(name.split(".")[0], 16))
            except ValueError:
                pass
    return indices


def is_staged_copy(file_id, chunk_index):
    """True for a temporary indexer copy that is not one of its assignments."""
    placement = chunk_placement_state.get(file_id)
    return bool(placement) and chunk_index in placement and (
        peer_id not in placement[chunk_index]
    )


def would_hold_full_file(file_id, chunk_index):
    """FIX 16: True if storing this chunk would give this peer the whole file.

    Counts stored chunks (minus temporary indexer copies) plus transfers
    already in progress. Join rebalancing keeps older replicas, so two
    slightly different membership views could otherwise add up to a full
    file on one peer, breaking "no single peer holds a complete file".
    """
    with file_catalog_lock:
        metadata = file_catalog_state.get(file_id)
        if metadata is None:
            return False
        chunk_count = metadata["chunk_count"]
    held = {
        index
        for index in local_chunk_indices(file_id)
        if not is_staged_copy(file_id, index)
    }
    with chunk_download_lock:
        held.update(
            index
            for task_file_id, index, _ in active_replication_tasks
            if task_file_id == file_id
        )
    held.add(chunk_index)
    return chunk_count >= 2 and len(held) >= chunk_count


def start_replication_request(file_id, chunk_index, owner_id, owner_addr):
    """Start a requested replica transfer once file metadata is available."""
    with file_catalog_lock:
        metadata = file_catalog_state.get(file_id)
        if metadata is None or chunk_index not in metadata["chunk_hashes"]:
            pending_replication_requests.add(
                (file_id, chunk_index, owner_id, owner_addr)
            )
            return

    if (
        not chunk_store.has_chunk(CHUNK_STORE_DIR, file_id, chunk_index)
        and would_hold_full_file(file_id, chunk_index)
    ):
        print(
            f"Declined replica of chunk {chunk_index} of {file_id.hex()[:8]}: "
            "it would put the complete file on this peer"
        )
        return

    task_key = (file_id, chunk_index, owner_id)
    with chunk_download_lock:
        if any(
            active_file_id == file_id
            and active_chunk_index == chunk_index
            for active_file_id, active_chunk_index, _
            in active_replication_tasks
        ):
            return
        active_replication_tasks.add(task_key)

    threading.Thread(
        target=replicate_chunk_from_peer,
        args=(file_id, chunk_index, owner_id, owner_addr),
        daemon=True,
    ).start()


def maybe_release_staged_chunk(file_id, chunk_index):
    """Drop a temporary source copy after all assigned replicas report ready."""
    placement = chunk_placement_state.get(file_id)
    if not placement or chunk_index not in placement:
        return False

    expected_owners = set(placement[chunk_index])
    if peer_id in expected_owners:
        return False

    with file_catalog_lock:
        actual_owners = file_catalog_state[file_id]["chunk_owners"][chunk_index]
        all_replicas_reported = expected_owners.issubset(actual_owners)
        # FIX 16: never delete a copy this peer has announced as its own.
        if peer_id in actual_owners:
            return False

    if all_replicas_reported:
        return chunk_store.delete_chunk(CHUNK_STORE_DIR, file_id, chunk_index)
    return False


def download_one_chunk(
    file_id,
    chunk_index,
    errors,
    owner_order,
    source_loads,
    source_load_lock,
    source_reservation,
    staging_dir,
):
    """Worker used by download_file to fetch one chunk independently."""
    worker_started = time.monotonic()
    trace_worker = os.environ.get("P2P_TEST_TRACE") == "1"
    if trace_worker:
        print(
            f"P2P_TEST_CHUNK_WORKER_START:{file_id.hex()}:{chunk_index}:"
            f"{worker_started:.9f}",
            flush=True,
        )
    try:
        source_id = request_chunk(
            file_id,
            chunk_index,
            owner_order=owner_order,
            source_loads=source_loads,
            source_load_lock=source_load_lock,
            source_reservation=source_reservation,
            store_dir=staging_dir,
            announce=False,
        )
        if os.environ.get("P2P_TEST_TRACE") == "1":
            print(
                f"P2P_TEST_CHUNK_SOURCE:{source_id}:{chunk_index}",
                flush=True,
            )
        return source_id
    except Exception as error:
        errors[chunk_index] = str(error)
        return None
    finally:
        if trace_worker:
            print(
                f"P2P_TEST_CHUNK_WORKER_END:{file_id.hex()}:{chunk_index}:"
                f"{time.monotonic():.9f}",
                flush=True,
            )


def current_chunk_owners(file_id, chunk_count):
    """Return a fresh snapshot of the catalog's owners for every chunk."""
    with file_catalog_lock:
        metadata = file_catalog_state[file_id]
        return {
            chunk_index: tuple(metadata["chunk_owners"][chunk_index])
            for chunk_index in range(chunk_count)
        }


def download_file(file_id, destination_path):
    """Fetch chunks in parallel, rebuild the file, and verify its SHA-256 ID.

    FIX 2: downloaded chunks go to a private staging folder that is deleted
    after assembly. They are never written to the shared chunk store and
    never announced with CHUNK_HAVE, so a downloader does not become an
    owner of every chunk (which would put a complete file on one peer).

    FIX 9: a chunk that fails is retried up to DOWNLOAD_CHUNK_ATTEMPTS times
    with a fresh owner snapshot, so a replica that appears later (repair) or
    a busy source can still finish the file.
    """
    with file_catalog_lock:
        metadata = file_catalog_state.get(file_id)
        if metadata is None:
            raise ValueError("File ID is not in the catalog")
        chunk_count = metadata["chunk_count"]
        filename = metadata["filename"]

    owners_by_chunk = current_chunk_owners(file_id, chunk_count)

    if not destination_path:
        destination_path = os.path.join("downloads", filename)

    staging_dir = os.path.join(
        DOWNLOAD_STAGING_DIR,
        f"{local_port}-{file_id.hex()[:16]}-{time.time_ns()}",
    )

    try:
        # Chunks this peer already owns are copied into the staging folder
        # so the shared store keeps only this peer's assigned share.
        missing_chunks = []
        for chunk_index in range(chunk_count):
            if chunk_store.has_chunk(CHUNK_STORE_DIR, file_id, chunk_index):
                chunk_store.store_chunk(
                    staging_dir,
                    file_id,
                    chunk_index,
                    chunk_store.load_chunk(CHUNK_STORE_DIR, file_id, chunk_index),
                )
            else:
                missing_chunks.append(chunk_index)

        errors = {}
        attempts = {chunk_index: 0 for chunk_index in missing_chunks}
        source_loads = {}
        source_load_lock = threading.Lock()
        active = {}
        with ThreadPoolExecutor(max_workers=4) as executor:
            while missing_chunks or active:
                while missing_chunks and len(active) < 4:
                    with source_load_lock:
                        available = [
                            chunk_index
                            for chunk_index in missing_chunks
                            if any(
                                source_loads.get(owner_id, 0) < 2
                                for owner_id in owners_by_chunk[chunk_index]
                            )
                        ]
                        if not available:
                            break
                        rarest_count = min(
                            len(owners_by_chunk[chunk_index])
                            for chunk_index in available
                        )
                        rarest = [
                            chunk_index
                            for chunk_index in available
                            if len(owners_by_chunk[chunk_index]) == rarest_count
                        ]
                        chunk_index = random.choice(rarest)
                        owner_order = tuple(
                            owner_id
                            for owner_id in sorted(
                                owners_by_chunk[chunk_index],
                                key=lambda owner_id: (
                                    owner_id != test_preferred_owner_id,
                                    source_loads.get(owner_id, 0),
                                    random.random(),
                                ),
                            )
                            if source_loads.get(owner_id, 0) < 2
                        )
                        primary_owner = owner_order[0] if owner_order else None
                        if primary_owner is not None:
                            source_loads[primary_owner] = (
                                source_loads.get(primary_owner, 0) + 1
                            )
                    if primary_owner is None:
                        missing_chunks.remove(chunk_index)
                        errors[chunk_index] = "No known chunk owners"
                        continue
                    attempts[chunk_index] += 1
                    errors.pop(chunk_index, None)
                    source_reservation = {"owner_id": primary_owner}
                    future = executor.submit(
                        download_one_chunk,
                        file_id,
                        chunk_index,
                        errors,
                        owner_order,
                        source_loads,
                        source_load_lock,
                        source_reservation,
                        staging_dir,
                    )
                    active[future] = (chunk_index, source_reservation)
                    missing_chunks.remove(chunk_index)

                if not active:
                    for chunk_index in missing_chunks:
                        errors[chunk_index] = "No available peer owns this chunk"
                    break

                completed, _ = wait(active, return_when=FIRST_COMPLETED)
                for future in completed:
                    chunk_index, source_reservation = active.pop(future)
                    actual_owner = future.result()
                    reserved_owner_id = source_reservation.get("owner_id")
                    if reserved_owner_id is not None:
                        with source_load_lock:
                            source_loads[reserved_owner_id] = max(
                                0,
                                source_loads.get(reserved_owner_id, 1) - 1,
                            )
                    if actual_owner is None:
                        if attempts[chunk_index] < DOWNLOAD_CHUNK_ATTEMPTS:
                            # Refresh owners: a crashed peer may have been
                            # removed and a repaired replica announced.
                            owners_by_chunk = current_chunk_owners(
                                file_id, chunk_count
                            )
                            missing_chunks.append(chunk_index)
                            print(
                                f"Retrying chunk {chunk_index} "
                                f"(attempt {attempts[chunk_index] + 1} of "
                                f"{DOWNLOAD_CHUNK_ATTEMPTS})"
                            )
                        elif chunk_index not in errors:
                            errors[chunk_index] = "No peer delivered this chunk"

        if errors:
            chunk_index = min(errors)
            raise TimeoutError(
                f"Could not download chunk {chunk_index}: {errors[chunk_index]}"
            )

        return chunk_store.assemble_file(
            staging_dir,
            file_id,
            chunk_count,
            destination_path,
        )
    finally:
        shutil.rmtree(staging_dir, ignore_errors=True)


def announce_chunk_have(file_id, chunk_index):
    """Announce a chunk after the local peer has actually stored its bytes."""
    with file_catalog_lock:
        file_catalog_helpers.add_chunk_owner(
            file_catalog_state,
            file_id,
            chunk_index,
            peer_id,
        )

    recipients = peer_snapshot()
    for destination_id, destination_addr in recipients:
        send_chunk_have(destination_id, destination_addr, file_id, chunk_index)
    return len(recipients)


def show_file_catalog():
    """Print indexed files and the known peer owners for each chunk."""
    with file_catalog_lock:
        records = file_catalog_helpers.file_list_records(file_catalog_state)
        details = [
            (
                file_id,
                metadata["file_size"],
                metadata["chunk_size"],
                metadata["chunk_count"],
                {
                    index: tuple(sorted(owners))
                    for index, owners in metadata["chunk_owners"].items()
                },
            )
            for file_id, metadata in file_catalog_state.items()
        ]

    if not records:
        print("File catalog is empty.")
        return

    for file_id, file_size, chunk_size, filename in records:
        entry = next(item for item in details if item[0] == file_id)
        print(
            f"{filename}: {file_size} bytes, {entry[3]} chunks "
            f"({chunk_size} bytes per chunk), file_id={file_id.hex()}"
        )
        for chunk_index, owners in entry[4].items():
            print(f"  chunk {chunk_index}: peers {owners}")


def queue_group_chat(text):
    """Queue this chat separately for every peer currently in the group."""
    # Validate the UTF-8 byte limit before placing the message in any queue.
    protocol_design.make_chat_payload(1, 1, text)
    if peer_id == 0 or not joined_network:
        print("Waiting to join the peer network before sending chat.")
        return

    recipients = peer_snapshot()

    if not recipients:
        print("No active peers to send the chat to.")
        return

    with chat_state_lock:
        for known_id, _ in recipients:
            outgoing_chat_queues.setdefault(known_id, deque()).append(text)


def clear_pending_chat(removed_peer_id):
    """Drop queued and unacknowledged chats after a peer leaves or times out."""
    with chat_state_lock:
        queue_for_peer = outgoing_chat_queues.pop(removed_peer_id, deque())
        pending = pending_chats.pop(removed_peer_id, {})

    # FIX 13: only report a failure when chat was actually outstanding.
    if pending or queue_for_peer:
        print(f"Chat delivery to peer {removed_peer_id} failed; peer was removed")


def chat_sender():
    """Run chat_sender_once forever (FIX 11: survive unexpected errors)."""
    while True:
        try:
            chat_sender_once()
        except Exception as error:
            print(f"Chat sender cycle failed: {error}")
        time.sleep(0.05)


def chat_sender_once():
    """Keep an eight-message per-peer window and retry each message by its RTO."""
    now = time.monotonic()

    for destination_id, destination_addr in peer_snapshot():
        datagrams_to_send = []

        with chat_state_lock:
            pending_by_sequence = pending_chats.setdefault(destination_id, {})
            for pending in pending_by_sequence.values():
                if now - pending["sent_at"] >= pending["timeout"]:
                    pending["sent_at"] = now
                    pending["retransmitted"] = True
                    pending["timeout"] = min(MAX_RTO, pending["timeout"] * 2)
                    datagrams_to_send.append(pending["datagram"])

            queue_for_peer = outgoing_chat_queues.get(destination_id)
            while queue_for_peer and len(pending_by_sequence) < CHAT_WINDOW:
                text = queue_for_peer.popleft()
                session_id = outgoing_chat_session(destination_id)
                packet_sequence = next_packet_sequence(destination_id)
                chat_sequence = next_chat_sequence(destination_id)
                datagram_to_send = protocol_design.make_datagram(
                    packet_name="CHAT",
                    peer_id=peer_id,
                    destination_p_id=destination_id,
                    protocol_version=protocol_design.PROTOCOL_VERSION,
                    flags=0,
                    group_id=0,
                    seq_no=packet_sequence,
                    payload=(session_id, chat_sequence, text),
                )
                pending_by_sequence[packet_sequence] = {
                        "seq_no": packet_sequence,
                    "datagram": datagram_to_send,
                    "addr": destination_addr,
                    "sent_at": now,
                    "timeout": get_peer_rto(destination_id),
                    "retransmitted": False,
                }
                datagrams_to_send.append(datagram_to_send)

        for datagram_to_send in datagrams_to_send:
            try:
                server.sendto(datagram_to_send, destination_addr)
            except OSError:
                pass


def chat_input():
    """Read chat lines and the small file-catalog commands from the terminal."""
    global test_preferred_owner_id
    while True:
        try:
            text = input()
            if text == "/files":
                show_file_catalog()
            elif text.startswith("/test-prefer-owner ") and os.environ.get("P2P_TEST_TRACE") == "1":
                test_preferred_owner_id = int(text.split(maxsplit=1)[1])
                print(
                    f"P2P_TEST_PREFERRED_OWNER_SET:{test_preferred_owner_id}",
                    flush=True,
                )
            elif text.startswith("/index "):
                file_path = text[len("/index "):].strip()
                if len(file_path) >= 2 and file_path[0] == file_path[-1] == '"':
                    file_path = file_path[1:-1]
                file_id, metadata, stored_count, replicas_requested = (
                    register_file_metadata(file_path)
                )
                print(
                    f"Stored {stored_count} chunks for {metadata['filename']} "
                    f"(file ID {file_id.hex()})."
                )
                if replicas_requested:
                    print(f"Requested {replicas_requested} remote chunk copies.")
                else:
                    print(
                        f"Distributed replication needs at least {MIN_DISTRIBUTION_PEERS} "
                        "joined peers "
                        "and a file with at least two chunks."
                    )
            elif text.startswith("/download "):
                command_parts = text.split(maxsplit=2)
                if len(command_parts) < 2:
                    raise ValueError("Usage: /download FILE_ID_HEX [output_path]")
                file_id = bytes.fromhex(command_parts[1])
                if len(file_id) != 32:
                    raise ValueError("FILE_ID_HEX must contain 64 hex characters")
                destination = command_parts[2] if len(command_parts) == 3 else None
                if (
                    destination
                    and len(destination) >= 2
                    and destination[0] == destination[-1] == '"'
                ):
                    destination = destination[1:-1]
                threading.Thread(
                    target=download_file_from_command,
                    args=(file_id, destination),
                    daemon=True,
                ).start()
            elif text:
                queue_group_chat(text)
        except (OSError, ValueError, UnicodeError) as error:
            print(f"Cannot process input: {error}")
        except EOFError:
            return


def download_file_from_command(file_id, destination_path):
    """Run the download outside the terminal input loop and report its result."""
    try:
        saved_path = download_file(file_id, destination_path)
        print(f"Downloaded and verified file: {saved_path}")
    except (OSError, ValueError, TimeoutError) as error:
        print(f"Download failed: {error}")


def receive():
    while True:
        try:
            message, addr = server.recvfrom(1024)
            messages.put((message, addr))
        except:
            pass


def id_generation():
    """Generate an ID that is unused in this peer's current group view."""
    used_ids = {known_id for known_id, _ in peer_snapshot()}
    if peer_id:
        used_ids.add(peer_id)
    available_ids = [candidate for candidate in range(1, 65535) if candidate not in used_ids]
    if not available_ids:
        raise RuntimeError("No unused peer IDs remain")
    return random.choice(available_ids)


def join_ack(joiner_addr, joining_id):
    entries = [(peer_id, advertise_ip, local_port)]

    for known_id, known_addr in peer_snapshot():
        if known_id != joining_id:
            entries.append((known_id, known_addr[0], known_addr[1]))
    # FIX 12: one JOIN_ACK holds at most 125 peer records (1,024-byte limit).
    # Peers left out still introduce themselves through NEW_PEER/HEARTBEAT.
    entries = entries[:MAX_JOIN_ACK_PEERS]

    datagram = protocol_design.make_datagram(
        packet_name="JOIN_ACK",
        peer_id=peer_id,
        destination_p_id=joining_id,
        protocol_version=protocol_design.PROTOCOL_VERSION,
        flags=0,
        group_id=0,
        seq_no=0,
        payload=entries,
    )
    server.sendto(datagram, joiner_addr)


def join(peer_id, bootstrap_addr):
    """Send a binary JOIN and retry it until the bootstrap assigns an ID."""
    datagram = protocol_design.make_datagram(
        packet_name="JOIN",
        peer_id=0,
        destination_p_id=0,
        protocol_version=protocol_design.PROTOCOL_VERSION,
        flags=0,
        group_id=0,
        seq_no=0,
        payload=None,
    )
    server.sendto(datagram, bootstrap_addr)
    threading.Thread(
        target=retry_join,
        args=(datagram, bootstrap_addr),
        daemon=True,
    ).start()


def retry_join(datagram, bootstrap_addr):
    """Repeat JOIN so a lost request or JOIN_ACK does not strand a newcomer."""
    for _ in range(JOIN_RETRIES):
        time.sleep(JOIN_ACK_TIMEOUT)
        if joined_network:
            return
        try:
            server.sendto(datagram, bootstrap_addr)
        except OSError:
            return
    time.sleep(JOIN_ACK_TIMEOUT)
    if not joined_network:
        print(
            f"No JOIN_ACK from {bootstrap_addr[0]}:{bootstrap_addr[1]}; "
            "check the bootstrap address and restart this peer."
        )


def leave(peer_id):
    """Notify every known peer with a binary LEAVE packet."""
    if peer_id == 0 or not joined_network:
        return
    for known_id, peer_addr in peer_snapshot():
        datagram = protocol_design.make_datagram(
            packet_name="LEAVE",
            peer_id=peer_id,
            destination_p_id=known_id,
            protocol_version=protocol_design.PROTOCOL_VERSION,
            flags=0,
            group_id=0,
            seq_no=0,
            payload=None,
        )
        server.sendto(datagram, peer_addr)


def heartbeat():
    """Run heartbeat_once forever; one failure must not stop heartbeats.

    FIX 11: the old loop had no exception handling, so a single unexpected
    error ended the thread and every other peer then timed this peer out.
    """
    while True:
        try:
            heartbeat_once()
        except Exception as error:
            print(f"Heartbeat cycle failed: {error}")
        time.sleep(HEARTBEAT_INTERVAL)


def heartbeat_once():
    """Send one round of heartbeats, expire silent peers, and run repairs."""
    if not joined_network or peer_id == 0:
        return

    peer_count, view_hash = peer_view_hash()

    for known_id, peer_addr in peer_snapshot():
        datagram = protocol_design.make_datagram(
            packet_name="HEARTBEAT",
            peer_id=peer_id,
            destination_p_id=known_id,
            protocol_version=protocol_design.PROTOCOL_VERSION,
            flags=0,
            group_id=0,
            seq_no=0,
            payload=(peer_count, view_hash),
        )
        try:
            server.sendto(datagram, peer_addr)
        except OSError:
            pass

    now = time.monotonic()

    expired_ids = [
        known_id
        for known_id, peer_addr in peer_snapshot()
        if known_id in last_seen
        and now - last_seen[known_id] > PEER_TIMEOUT
    ]

    for expired_id in expired_ids:
        remove_peer(expired_id)
        last_seen.pop(expired_id, None)
        print(f"Peer {expired_id} timed out and was removed")

    repair_under_replicated_chunks(force=bool(expired_ids))
    retry_initial_replications()


def handle_membership_packet(message, addr):
    """Handle all binary membership packets and repair peer-list differences."""
    global peer_id, joined_network

    if len(message) < protocol_design.header_size:
        return False

    packet_type = message[4]
    membership_types = {
        protocol_design.get_packet_type(name)
        for name in (
            "JOIN",
            "JOIN_ACK",
            "LEAVE",
            "HEARTBEAT",
            "NEW_PEER",
            "PEER_LIST_REQ",
        )
    }
    if packet_type not in membership_types:
        return False

    try:
        fields = struct.unpack(
            protocol_design.HEADER_FORMAT,
            message[:protocol_design.header_size],
        )
        sender_id, destination_id = fields[0], fields[1]
        request_type = protocol_design.get_packet_type("PEER_LIST_REQ")
        join_type = protocol_design.get_packet_type("JOIN")
        join_ack_type = protocol_design.get_packet_type("JOIN_ACK")
        leave_type = protocol_design.get_packet_type("LEAVE")
        heartbeat_type = protocol_design.get_packet_type("HEARTBEAT")
        new_peer_type = protocol_design.get_packet_type("NEW_PEER")

        if packet_type == join_type:
            protocol_design.check_datagram(message, show_output=False)
            if peer_id == 0 or not joined_network:
                return True

            existing_id = next(
                (
                    known_id
                    for known_id, known_addr in peer_snapshot()
                    if known_addr == addr
                ),
                None,
            )
            newly_joined = existing_id is None
            joining_id = existing_id if existing_id is not None else id_generation()
            if newly_joined:
                while not add_peer(joining_id, addr):
                    joining_id = id_generation()
                last_seen[joining_id] = time.monotonic()

            join_ack(addr, joining_id)
            if newly_joined:
                for known_id, known_addr in peer_snapshot():
                    if known_id == joining_id:
                        continue
                    new_peer_datagram = protocol_design.make_datagram(
                        packet_name="NEW_PEER",
                        peer_id=peer_id,
                        destination_p_id=known_id,
                        protocol_version=protocol_design.PROTOCOL_VERSION,
                        flags=0,
                        group_id=0,
                        seq_no=0,
                        payload=(joining_id, addr[0], addr[1]),
                    )
                    server.sendto(new_peer_datagram, known_addr)
                send_file_catalog_to_peer(joining_id, addr)
            return True

        if (
            packet_type == join_ack_type
            and not joined_network
            and bootstrap_addr == addr
            and destination_id not in (0, 65535)
        ):
            peer_records = protocol_design.check_datagram(
                message,
                show_output=False,
            )
            peer_id = destination_id
            if os.environ.get("P2P_TEST_TRACE") == "1":
                print(f"P2P_TEST_ASSIGNED_ID:{peer_id}", flush=True)
            added_peers = []
            for known_id, known_ip, known_port in peer_records:
                # FIX 6: the responder's own record may advertise 127.0.0.1;
                # trust the UDP source address it actually replied from.
                known_addr = addr if known_id == sender_id else (known_ip, known_port)
                if known_id != peer_id and add_peer(known_id, known_addr):
                    last_seen[known_id] = time.monotonic()
                    added_peers.append((known_id, known_addr))

            joined_network = True
            print("Updated peer list:", peer_snapshot())
            broadcast_file_catalog()
            rebalance_chunks_for_local_peer()
            return True

        if destination_id != peer_id:
            return True

        known_sender = any(
            known_id == sender_id and known_addr == addr
            for known_id, known_addr in peer_snapshot()
        )
        if packet_type == request_type:
            if not known_sender:
                return True
            protocol_design.check_datagram(message, show_output=False)
            peer_records = [(peer_id, advertise_ip, local_port)]
            peer_records.extend(
                (known_id, known_addr[0], known_addr[1])
                for known_id, known_addr in peer_snapshot()
            )
            peer_records = peer_records[:MAX_JOIN_ACK_PEERS]  # FIX 12
            response = protocol_design.make_datagram(
                packet_name="JOIN_ACK",
                peer_id=peer_id,
                destination_p_id=sender_id,
                protocol_version=protocol_design.PROTOCOL_VERSION,
                flags=0,
                group_id=0,
                seq_no=0,
                payload=peer_records,
            )
            server.sendto(response, addr)
            return True

        if packet_type == heartbeat_type:
            peer_count, view_hash = protocol_design.check_datagram(
                message,
                show_output=False,
            )
            if sender_id == 0 or sender_id == peer_id:
                return True
            known_addr = dict(peer_snapshot()).get(sender_id)
            if known_addr is not None and known_addr != addr:
                return True
            if known_addr is None:
                if not add_peer(sender_id, addr):
                    return True
                last_seen[sender_id] = time.monotonic()
                send_file_catalog_to_peer(sender_id, addr)
                rebalance_chunks_for_local_peer()
            else:
                last_seen[sender_id] = time.monotonic()

            local_count, local_hash = peer_view_hash()
            if (peer_count, view_hash) == (local_count, local_hash):
                peer_view_mismatch_counts.pop(sender_id, None)
            else:
                mismatch_count = peer_view_mismatch_counts.get(sender_id, 0) + 1
                peer_view_mismatch_counts[sender_id] = mismatch_count
                if mismatch_count >= 2:
                    send_peer_list_request(sender_id, addr)
                    peer_view_mismatch_counts[sender_id] = 0
            return True

        if not known_sender:
            return True

        last_seen[sender_id] = time.monotonic()
        if packet_type == join_ack_type:
            peer_records = protocol_design.check_datagram(message, show_output=False)
            added_peers = []
            for known_id, known_ip, known_port in peer_records:
                # FIX 6: use the responder's real UDP source address.
                known_addr = addr if known_id == sender_id else (known_ip, known_port)
                if known_id != peer_id and add_peer(known_id, known_addr):
                    last_seen[known_id] = time.monotonic()
                    added_peers.append((known_id, known_addr))
            for known_id, known_addr in added_peers:
                send_file_catalog_to_peer(known_id, known_addr)
            if added_peers:
                rebalance_chunks_for_local_peer()
            return True

        if packet_type == new_peer_type:
            known_id, known_ip, known_port = protocol_design.check_datagram(
                message,
                show_output=False,
            )
            known_addr = (known_ip, known_port)
            if known_id != peer_id and add_peer(known_id, known_addr):
                last_seen[known_id] = time.monotonic()
                send_file_catalog_to_peer(known_id, known_addr)
                rebalance_chunks_for_local_peer()
            return True

        if packet_type == leave_type:
            protocol_design.check_datagram(message, show_output=False)
            if remove_peer(sender_id, addr):
                repair_under_replicated_chunks(force=True)
            return True

        if packet_type == join_type:
            return True
        return True
    except (ValueError, OSError) as error:
        print(f"Ignoring invalid membership packet from {addr}: {error}")
        return True


def handle_protocol_chat_or_ack(message, addr):
    """Validate CHAT/ACK datagrams; ACK valid CHAT packets and never ACK ACKs."""
    if len(message) < protocol_design.header_size:
        return False

    packet_type = message[4]
    if packet_type not in (protocol_design.get_packet_type("CHAT"),
                           protocol_design.get_packet_type("ACK")):
        return False

    try:
        if packet_type == protocol_design.get_packet_type("CHAT"):
            fields = struct.unpack(
                protocol_design.HEADER_FORMAT,
                message[:protocol_design.header_size],
            )
            sender_id = fields[0]
            destination_id = fields[1]
            packet_sequence = fields[6]

            # Validate version, checksum, header/payload lengths, and UTF-8.
            session_id, chat_sequence, chat_text = protocol_design.check_datagram(
                message,
                show_output=False,
            )

            known_sender = any(
                known_id == sender_id and known_addr == addr
                for known_id, known_addr in peer_snapshot()
            )

            if destination_id != peer_id or not known_sender:
                return True

            status = classify_incoming_chat(
                sender_id,
                session_id,
                packet_sequence,
                chat_sequence,
            )

            if status in ("accepted", "duplicate"):
                ack_datagram = protocol_design.make_ack_for_chat(message, peer_id)
                server.sendto(ack_datagram, addr)

                if status == "accepted":
                    print(
                        f"Chat from peer {sender_id} "
                        f"(sequence {chat_sequence}): {chat_text}"
                    )
        else:
            ack_no = protocol_design.check_datagram(message, show_output=False)
            ack_fields = struct.unpack(
                protocol_design.HEADER_FORMAT,
                message[:protocol_design.header_size],
            )
            sender_id = ack_fields[0]
            destination_id = ack_fields[1]
            matching_peer = next(
                (
                    (known_id, known_addr)
                    for known_id, known_addr in peer_snapshot()
                    if known_id == sender_id and known_addr == addr
                ),
                None,
            )

            if destination_id == peer_id and matching_peer is not None:
                with chat_state_lock:
                    pending_by_sequence = pending_chats.get(sender_id)
                    pending = (
                        pending_by_sequence.get(ack_no)
                        if pending_by_sequence is not None
                        else None
                    )
                    if pending is not None and pending["addr"] == addr:
                        pending_by_sequence.pop(ack_no, None)
                        if not pending["retransmitted"]:
                            update_peer_rtt(
                                sender_id,
                                time.monotonic() - pending["sent_at"],
                            )
                        if not pending_by_sequence:
                            pending_chats.pop(sender_id, None)
    except (ValueError, UnicodeDecodeError) as error:
        print(f"Ignoring invalid protocol packet from {addr}: {error}")

    return True


def handle_protocol_catalog(message, addr):
    """Process FILE_LIST, chunk hashes, and chunk ownership catalog messages."""
    if len(message) < protocol_design.header_size:
        return False

    packet_type = message[4]
    file_list_type = protocol_design.get_packet_type("FILE_LIST")
    chunk_have_type = protocol_design.get_packet_type("CHUNK_HAVE")
    chunk_hash_type = protocol_design.get_packet_type("CHUNK_HASH")
    chunk_hash_ack_type = protocol_design.get_packet_type("CHUNK_HASH_ACK")
    file_list_ack_type = protocol_design.get_packet_type("FILE_LIST_ACK")
    chunk_have_ack_type = protocol_design.get_packet_type("CHUNK_HAVE_ACK")
    if packet_type not in (
        file_list_type,
        chunk_have_type,
        chunk_hash_type,
        chunk_hash_ack_type,
        file_list_ack_type,
        chunk_have_ack_type,
    ):
        return False

    try:
        fields = struct.unpack(
            protocol_design.HEADER_FORMAT,
            message[:protocol_design.header_size],
        )
        sender_id, destination_id = fields[0], fields[1]

        if packet_type == file_list_type:
            file_records = protocol_design.check_datagram(message, show_output=False)
        elif packet_type == file_list_ack_type:
            file_id = protocol_design.check_datagram(message, show_output=False)
        elif packet_type in (
            chunk_have_type,
            chunk_hash_ack_type,
            chunk_have_ack_type,
        ):
            file_id, chunk_index = protocol_design.check_datagram(
                message, show_output=False
            )
        else:
            file_id, chunk_index, chunk_hash = protocol_design.check_datagram(
                message,
                show_output=False,
            )

        if destination_id != peer_id:
            return True

        known_sender = any(
            known_id == sender_id and known_addr == addr
            for known_id, known_addr in peer_snapshot()
        )
        if not known_sender and bootstrap_addr == addr:
            add_peer(sender_id, addr)
            last_seen[sender_id] = time.monotonic()
            known_sender = True
        if not known_sender:
            return True

        if packet_type == chunk_hash_ack_type:
            hash_key = (sender_id, file_id, chunk_index)
            with chunk_hash_lock:
                hash_state = pending_chunk_hash_acks.get(hash_key)
                if hash_state is not None and hash_state["addr"] == addr:
                    pending_chunk_hash_acks.pop(hash_key, None)
            return True

        if packet_type == file_list_ack_type:
            file_list_key = (sender_id, file_id)
            with file_list_lock:
                file_list_state = pending_file_list_acks.get(file_list_key)
                if file_list_state is not None and file_list_state["addr"] == addr:
                    pending_file_list_acks.pop(file_list_key, None)
            return True

        if packet_type == chunk_have_ack_type:
            chunk_have_key = (sender_id, file_id, chunk_index)
            with chunk_have_lock:
                chunk_have_state = pending_chunk_have_acks.get(chunk_have_key)
                if chunk_have_state is not None and chunk_have_state["addr"] == addr:
                    pending_chunk_have_acks.pop(chunk_have_key, None)
            return True

        if packet_type == file_list_type:
            learned_owners = []
            ready_replications = []
            with file_catalog_lock:
                file_catalog_helpers.merge_file_list(file_catalog_state, file_records)
                observed_at = time.monotonic()
                for file_id, _, _, _ in file_records:
                    file_catalog_seen_at.setdefault(file_id, observed_at)
                for pending_key, pending_hash in tuple(pending_chunk_hashes.items()):
                    pending_file_id, pending_index = pending_key
                    if pending_file_id in file_catalog_state:
                        try:
                            file_catalog_helpers.set_chunk_hash(
                                file_catalog_state,
                                pending_file_id,
                                pending_index,
                                pending_hash,
                            )
                        except ValueError:
                            pass
                        pending_chunk_hashes.pop(pending_key, None)
                for pending_item in tuple(pending_chunk_have):
                    pending_file_id, pending_index, owner_id = pending_item
                    if pending_file_id in file_catalog_state:
                        try:
                            is_new = file_catalog_helpers.add_chunk_owner(
                                file_catalog_state,
                                pending_file_id,
                                pending_index,
                                owner_id,
                            )
                            if is_new:
                                learned_owners.append(pending_item)
                        except ValueError:
                            pass
                        pending_chunk_have.discard(pending_item)

                for pending_request in tuple(pending_replication_requests):
                    pending_file_id, pending_index, owner_id, owner_addr = pending_request
                    pending_metadata = file_catalog_state.get(pending_file_id)
                    if (
                        pending_metadata is not None
                        and pending_index in pending_metadata["chunk_hashes"]
                    ):
                        ready_replications.append(pending_request)
                        pending_replication_requests.discard(pending_request)

            for file_record in file_records:
                send_file_list_ack(sender_id, addr, file_record[0])

            print(
                f"Received FILE_LIST from peer {sender_id}: "
                f"{len(file_records)} file record(s)"
            )
            for learned_file_id, learned_index, owner_id in learned_owners:
                print(
                    f"Learned chunk {learned_index} of "
                    f"{learned_file_id.hex()[:8]} from peer {owner_id}"
                )
            for pending_file_id, pending_index, owner_id, owner_addr in ready_replications:
                start_replication_request(
                    pending_file_id,
                    pending_index,
                    owner_id,
                    owner_addr,
                )
            rebalance_chunks_for_local_peer()
        elif packet_type == chunk_have_type:
            with file_catalog_lock:
                if file_id in file_catalog_state:
                    is_new = file_catalog_helpers.add_chunk_owner(
                        file_catalog_state,
                        file_id,
                        chunk_index,
                        sender_id,
                    )
                else:
                    pending_chunk_have.add((file_id, chunk_index, sender_id))
                    is_new = False

            send_chunk_have_ack(sender_id, addr, file_id, chunk_index)

            if is_new:
                print(
                    f"Learned chunk {chunk_index} of {file_id.hex()[:8]} "
                    f"from peer {sender_id}"
                )
                maybe_release_staged_chunk(file_id, chunk_index)
            rebalance_chunks_for_local_peer()
        else:
            ready_replications = []
            with file_catalog_lock:
                if file_id in file_catalog_state:
                    file_catalog_helpers.set_chunk_hash(
                        file_catalog_state,
                        file_id,
                        chunk_index,
                        chunk_hash,
                    )
                else:
                    pending_chunk_hashes[(file_id, chunk_index)] = chunk_hash

                metadata = file_catalog_state.get(file_id)
                if metadata is not None and chunk_index in metadata["chunk_hashes"]:
                    for pending_request in tuple(pending_replication_requests):
                        request_file_id, request_index, _, _ = pending_request
                        if request_file_id == file_id and request_index == chunk_index:
                            ready_replications.append(pending_request)
                            pending_replication_requests.discard(pending_request)

            for pending_file_id, pending_index, owner_id, owner_addr in ready_replications:
                start_replication_request(
                    pending_file_id,
                    pending_index,
                    owner_id,
                    owner_addr,
                )
            send_chunk_hash_ack(sender_id, addr, file_id, chunk_index)
            rebalance_chunks_for_local_peer()

    except (ValueError, UnicodeDecodeError, KeyError) as error:
        print(f"Ignoring invalid catalog packet from {addr}: {error}")

    return True


def handle_chunk_transfer(message, addr):
    """Serve CHUNK_REQUEST or collect a requested CHUNK_DATA fragment."""
    if len(message) < protocol_design.header_size:
        return False

    packet_type = message[4]
    request_type = protocol_design.get_packet_type("CHUNK_REQUEST")
    data_type = protocol_design.get_packet_type("CHUNK_DATA")
    replication_type = protocol_design.get_packet_type("REPLICATE_REQ")
    ack_type = protocol_design.get_packet_type("CHUNK_ACK")
    nak_type = protocol_design.get_packet_type("CHUNK_NAK")
    if packet_type not in (
        request_type,
        data_type,
        replication_type,
        ack_type,
        nak_type,
    ):
        return False

    try:
        fields = struct.unpack(
            protocol_design.HEADER_FORMAT,
            message[:protocol_design.header_size],
        )
        sender_id, destination_id = fields[0], fields[1]
        if destination_id != peer_id:
            return True

        known_sender = any(
            known_id == sender_id and known_addr == addr
            for known_id, known_addr in peer_snapshot()
        )
        if not known_sender:
            return True

        if packet_type == replication_type:
            file_id, chunk_index = protocol_design.check_datagram(
                message,
                show_output=False,
            )
            start_replication_request(
                file_id,
                chunk_index,
                sender_id,
                addr,
            )
            return True

        if packet_type == ack_type:
            file_id, chunk_index, cumulative_offset, bitmap = (
                protocol_design.check_datagram(message, show_output=False)
            )
            transfer_key = (sender_id, file_id, chunk_index)
            rtt_sample = None
            ack_time = time.monotonic()
            with chunk_send_lock:
                send_state = pending_chunk_sends.get(transfer_key)
                if send_state is None or send_state["addr"] != addr:
                    return True

                total_size = send_state["total_size"]
                if (
                    cumulative_offset > total_size
                    or (
                        cumulative_offset != total_size
                        and cumulative_offset % CHUNK_FRAGMENT_DATA_SIZE != 0
                    )
                ):
                    return True

                for fragment_offset, _ in send_state["fragments"]:
                    newly_acked = False
                    if fragment_offset < cumulative_offset:
                        newly_acked = fragment_offset not in send_state["acked"]
                        send_state["acked"].add(fragment_offset)
                    elif fragment_offset >= cumulative_offset:
                        bit_index, remainder = divmod(
                            fragment_offset - cumulative_offset,
                            CHUNK_FRAGMENT_DATA_SIZE,
                        )
                        if (
                            remainder == 0
                            and bit_index < CHUNK_SEND_WINDOW
                            and bitmap & (1 << bit_index)
                        ):
                            newly_acked = fragment_offset not in send_state["acked"]
                            send_state["acked"].add(fragment_offset)
                    if (
                        newly_acked
                        and fragment_offset in send_state["first_sent"]
                        and fragment_offset not in send_state["retransmitted"]
                    ):
                        if rtt_sample is None:
                            rtt_sample = (
                                ack_time - send_state["first_sent"][fragment_offset]
                            )
                send_state["event"].set()
            if rtt_sample is not None:
                update_peer_rtt(sender_id, rtt_sample)
            return True

        if packet_type == nak_type:
            file_id, chunk_index, reason = protocol_design.check_datagram(
                message,
                show_output=False,
            )
            if reason == protocol_design.CHUNK_NAK_REASONS["BUSY"]:
                # BUSY is transient: an upload may already be sending this
                # chunk. Keep the current receive state so its fragments can
                # arrive; a later CHUNK_REQUEST can retry if needed.
                return True
            key = (file_id, chunk_index)
            with chunk_download_lock:
                state = pending_chunk_downloads.get(key)
                if (
                    state is not None
                    and state["owner_id"] == sender_id
                    and state["owner_addr"] == addr
                ):
                    state["error"] = (
                        f"Peer rejected chunk request (reason {reason})"
                    )
                    state["error_reason"] = reason
                    state["event"].set()
            if reason == protocol_design.CHUNK_NAK_REASONS["HASH_MISMATCH"]:
                transfer_key = (sender_id, file_id, chunk_index)
                with chunk_send_lock:
                    send_state = pending_chunk_sends.get(transfer_key)
                    if send_state is not None and send_state["addr"] == addr:
                        send_state["error"] = "Receiver reported a chunk hash mismatch"
                        send_state["event"].set()
            return True

        if packet_type == request_type:
            file_id, chunk_index, start_offset, length = (
                protocol_design.check_datagram(message, show_output=False)
            )
            if chunk_store.has_chunk(CHUNK_STORE_DIR, file_id, chunk_index):
                chunk_data = chunk_store.load_chunk(
                    CHUNK_STORE_DIR,
                    file_id,
                    chunk_index,
                )
                if (
                    start_offset % CHUNK_FRAGMENT_DATA_SIZE != 0
                    or length <= 0
                    or start_offset + length > len(chunk_data)
                ):
                    send_chunk_nak(
                        sender_id,
                        addr,
                        file_id,
                        chunk_index,
                        protocol_design.CHUNK_NAK_REASONS["NOT_STORED"],
                    )
                    return True
                global test_busy_reject_used
                if (
                    os.environ.get("P2P_TEST_BUSY_FIRST_CHUNK_REQUEST") == "1"
                    and not test_busy_reject_used
                ):
                    test_busy_reject_used = True
                    send_chunk_nak(
                        sender_id,
                        addr,
                        file_id,
                        chunk_index,
                        protocol_design.CHUNK_NAK_REASONS["BUSY"],
                    )
                    print(
                        f"P2P_TEST_FORCED_BUSY:{sender_id}:{file_id.hex()}:{chunk_index}",
                        flush=True,
                    )
                    return True
                if not active_chunk_send_slots.acquire(blocking=False):
                    send_chunk_nak(
                        sender_id,
                        addr,
                        file_id,
                        chunk_index,
                        protocol_design.CHUNK_NAK_REASONS["BUSY"],
                    )
                    return True
                if os.environ.get("P2P_TEST_TRACE") == "1":
                    print(
                        f"P2P_TEST_SERVE_REQUEST:{sender_id}:"
                        f"{file_id.hex()}:{chunk_index}",
                        flush=True,
                    )
                try:
                    threading.Thread(
                        target=send_chunk_data_with_slot,
                        args=(sender_id, addr, file_id, chunk_index, start_offset, length),
                        daemon=True,
                    ).start()
                except (OSError, RuntimeError):
                    active_chunk_send_slots.release()
                    send_chunk_nak(
                        sender_id,
                        addr,
                        file_id,
                        chunk_index,
                        protocol_design.CHUNK_NAK_REASONS["BUSY"],
                    )
            else:
                send_chunk_nak(
                    sender_id,
                    addr,
                    file_id,
                    chunk_index,
                    protocol_design.CHUNK_NAK_REASONS["NOT_STORED"],
                )
            return True

        file_id, chunk_index, fragment_offset, total_chunk_size, fragment_data = (
            protocol_design.check_datagram(message, show_output=False)
        )
        key = (file_id, chunk_index)
        with chunk_download_lock:
            state = pending_chunk_downloads.get(key)
            if (
                state is None
                or state["owner_id"] != sender_id
                or state["owner_addr"] != addr
            ):
                return True

            if total_chunk_size != state["expected_size"]:
                state["error"] = "Peer sent a chunk with the wrong size"
                state["event"].set()
                return True

            if state["total_size"] not in (None, total_chunk_size):
                state["error"] = "Fragments disagree about total chunk size"
                state["event"].set()
                return True

            state["total_size"] = total_chunk_size
            expected_fragment_size = min(
                CHUNK_FRAGMENT_DATA_SIZE,
                total_chunk_size - fragment_offset,
            )
            if (
                fragment_offset % CHUNK_FRAGMENT_DATA_SIZE != 0
                or fragment_offset >= total_chunk_size
                or len(fragment_data) != expected_fragment_size
            ):
                return True

            prior_data = state["fragments"].get(fragment_offset)
            if prior_data is not None and prior_data != fragment_data:
                state["error"] = "Conflicting duplicate fragment"
                state["event"].set()
                return True
            if prior_data is None:
                state["last_progress"] = time.monotonic()
            state["fragments"][fragment_offset] = fragment_data

            expected_offset = 0
            ordered_fragments = []
            for offset, data in sorted(state["fragments"].items()):
                if offset != expected_offset:
                    send_chunk_ack(
                        sender_id,
                        addr,
                        file_id,
                        chunk_index,
                        state["fragments"],
                    )
                    return True
                ordered_fragments.append(
                    (file_id, chunk_index, offset, total_chunk_size, data)
                )
                expected_offset += len(data)

            if expected_offset == total_chunk_size:
                rebuilt_chunk = protocol_design.reassemble_chunk(ordered_fragments)
                if hashlib.sha256(rebuilt_chunk).digest() != state["expected_hash"]:
                    state["error"] = "Received chunk SHA-256 did not match catalog"
                    state["error_reason"] = protocol_design.CHUNK_NAK_REASONS["HASH_MISMATCH"]
                    if os.environ.get("P2P_TEST_TRACE") == "1":
                        print(
                            f"P2P_TEST_HASH_MISMATCH:{file_id.hex()}:"
                            f"{chunk_index}:{sender_id}",
                            flush=True,
                        )
                    send_chunk_nak(
                        sender_id,
                        addr,
                        file_id,
                        chunk_index,
                        protocol_design.CHUNK_NAK_REASONS["HASH_MISMATCH"],
                    )
                    state["event"].set()
                    return True

                chunk_store.store_chunk(
                    state["store_dir"],
                    file_id,
                    chunk_index,
                    rebuilt_chunk,
                )
                if os.environ.get("P2P_TEST_TRACE") == "1":
                    print(
                        f"P2P_TEST_CHUNK_RECEIVED:{sender_id}:"
                        f"{file_id.hex()}:{chunk_index}",
                        flush=True,
                    )
                send_chunk_ack(
                    sender_id,
                    addr,
                    file_id,
                    chunk_index,
                    state["fragments"],
                )
                state["event"].set()
            else:
                send_chunk_ack(
                    sender_id,
                    addr,
                    file_id,
                    chunk_index,
                    state["fragments"],
                )

    except (OSError, ValueError, UnicodeDecodeError) as error:
        print(f"Ignoring invalid chunk packet from {addr}: {error}")

    return True


def broadcast():
    while True:
        message, addr = messages.get()

        try:
            if handle_membership_packet(message, addr):
                continue
            if handle_protocol_chat_or_ack(message, addr):
                continue
            if handle_chunk_transfer(message, addr):
                continue
            if handle_protocol_catalog(message, addr):
                continue

            try:
                print(message.decode("utf-8"))
            except UnicodeDecodeError:
                print(f"Ignoring unrecognized binary message from {addr}")

        except UnicodeDecodeError:
            print(f"Ignoring invalid message from {addr}")
        finally:
            messages.task_done()

def test_peer_helpers():
    first_receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    second_receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    first_receiver.bind(("127.0.0.1", 0))
    second_receiver.bind(("127.0.0.1", 0))
    first_receiver.settimeout(2)
    second_receiver.settimeout(2)

    original_peers = peer_snapshot()

    try:
        with peer_lock:
            peers.clear()

        first_addr = first_receiver.getsockname()
        second_addr = second_receiver.getsockname()

        assert add_peer(65001, first_addr)
        first_snapshot = peer_snapshot()

        assert add_peer(65002, second_addr)
        assert not add_peer(65001, first_addr)
        assert first_snapshot == ((65001, first_addr),)

        test_datagram = b"rule-1-routing-test"
        recipients = send_chat_to_active_peers(test_datagram)

        assert recipients == (
            (65001, first_addr),
            (65002, second_addr),
        )
        assert first_receiver.recvfrom(1024)[0] == test_datagram
        assert second_receiver.recvfrom(1024)[0] == test_datagram

        assert not remove_peer(65001, second_addr)
        assert remove_peer(65001, first_addr)
        assert remove_peer(65002)
        assert peer_snapshot() == ()

        print("Peer helper tests passed.")

    finally:
        with peer_lock:
            peers[:] = original_peers

        first_receiver.close()
        second_receiver.close()


def detect_advertise_ip(bootstrap):
    """Return the local IPv4 address used to reach the bootstrap (FIX 6)."""
    if bootstrap is None:
        return "127.0.0.1"
    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        probe.connect(bootstrap)  # UDP connect sends nothing
        return probe.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        probe.close()


def main():
    global server, local_port, bootstrap_addr
    global advertise_ip, peer_id, joined_network

    arguments = sys.argv[1:]
    run_self_test = "--selftest" in arguments
    arguments = [argument for argument in arguments if argument != "--selftest"]
    explicit_ip = None
    if "--ip" in arguments:
        ip_index = arguments.index("--ip")
        if ip_index + 1 >= len(arguments):
            raise SystemExit("Usage: python p2p_file.py PORT [HOST:PORT] [--ip ADDRESS] [--selftest]")
        explicit_ip = arguments[ip_index + 1]
        del arguments[ip_index:ip_index + 2]
    if not arguments:
        raise SystemExit("Usage: python p2p_file.py PORT [HOST:PORT] [--ip ADDRESS] [--selftest]")

    local_port = int(arguments[0])
    # FIX 15: every peer gets its own chunk store. With one shared
    # peer_chunks/ folder, peers started from the same directory saw each
    # other's files, so has_chunk() was true everywhere and the distributed
    # storage, replication and failover paths were never really exercised.
    global CHUNK_STORE_DIR
    CHUNK_STORE_DIR = os.path.join(BASE_STORE_DIR, str(local_port))
    bootstrap_addr = None
    joined_network = False

    if len(arguments) > 1:
        host, port = arguments[1].rsplit(":", 1)
        # FIX 6: resolve names such as "localhost" so the JOIN_ACK source
        # address comparison (an IP tuple) matches the configured bootstrap.
        bootstrap_addr = (socket.gethostbyname(host), int(port))

    # FIX 6: advertise a reachable address instead of a hard-coded loopback.
    advertise_ip = explicit_ip or detect_advertise_ip(bootstrap_addr)

    server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server.setsockopt(
        socket.SOL_SOCKET,
        socket.SO_RCVBUF,
        SOCKET_RECEIVE_BUFFER_SIZE,
    )
    server.bind(("0.0.0.0", local_port))
    # Windows reports an ICMP "port unreachable" from an earlier sendto as an
    # error on the next recvfrom (WSAECONNRESET). Turn that off so a crashed
    # peer cannot disturb the receive loop.
    if hasattr(socket, "SIO_UDP_CONNRESET"):
        try:
            server.ioctl(socket.SIO_UDP_CONNRESET, False)
        except (OSError, ValueError):
            pass

    peer_id = 0 if bootstrap_addr is not None else id_generation()
    if bootstrap_addr is None:
        joined_network = True
    if os.environ.get("P2P_TEST_TRACE") == "1" and bootstrap_addr is None:
        print(f"P2P_TEST_ASSIGNED_ID:{peer_id}", flush=True)

    # FIX 14: the routing self-test mutates the global peer list, so it now
    # runs only on request instead of on every start.
    if run_self_test:
        test_peer_helpers()

    threading.Thread(target=receive, daemon=True).start()
    threading.Thread(target=broadcast, daemon=True).start()
    threading.Thread(target=heartbeat, daemon=True).start()
    threading.Thread(target=chat_sender, daemon=True).start()
    threading.Thread(target=chat_input, daemon=True).start()
    threading.Thread(target=retry_chunk_hashes, daemon=True).start()
    threading.Thread(target=retry_file_lists, daemon=True).start()
    threading.Thread(target=retry_chunk_have, daemon=True).start()

    if bootstrap_addr is not None:
        join(peer_id, bootstrap_addr)
    else:
        print("Started as the first peer")

    print("Peer is running on port", local_port)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        leave(peer_id)

if __name__ == "__main__":
    main()
