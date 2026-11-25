"""Unit tests for the fixes listed in FIXES.md.

Run from the project folder:  python -m unittest discover -s tests -v
"""

import os
import struct
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import p2p_file  # noqa: E402
import protocol_design as pd  # noqa: E402


def build(packet_name, payload, seq_no=0, ack_no=0, source=1001, dest=1002):
    return pd.make_datagram(
        packet_name, source, dest, pd.PROTOCOL_VERSION, 0, 0, seq_no, payload, ack_no
    )


def patch_header(datagram, index, value):
    """Change one header field and recompute the checksum."""
    fields = list(struct.unpack(pd.HEADER_FORMAT, datagram[:pd.header_size]))
    fields[index] = value
    fields[9] = 0
    raw = struct.pack(pd.HEADER_FORMAT, *fields) + datagram[pd.header_size:]
    value = pd.compute_checksum(raw)
    return raw[:pd.CHECKSUM_OFFSET] + struct.pack("!H", value) + raw[pd.CHECKSUM_OFFSET + 2:]


class ProtocolConformanceTests(unittest.TestCase):
    """FIX 5 and FIX 7: header rules and the version 2 CHAT payload."""

    def test_builtin_protocol_self_test_passes(self):
        pd.main()

    def test_chat_payload_round_trip_with_session(self):
        datagram = build("CHAT", (77, 5, "hello"), seq_no=3)
        self.assertEqual(pd.check_datagram(datagram, show_output=False), (77, 5, "hello"))

    def test_chat_text_limit_is_996_bytes(self):
        self.assertEqual(pd.MAX_CHAT_TEXT_BYTES, 996)
        self.assertEqual(len(build("CHAT", (1, 1, "A" * 996), seq_no=1)), 1024)
        with self.assertRaises(ValueError):
            pd.make_chat_payload(1, 1, "A" * 997)

    def test_zero_session_rejected(self):
        with self.assertRaises(ValueError):
            pd.make_chat_payload(0, 1, "x")

    def test_unknown_packet_type_rejected(self):
        datagram = patch_header(build("PEER_LIST_REQ", None), 2, 42)
        with self.assertRaises(ValueError):
            pd.check_datagram(datagram, show_output=False)

    def test_nonzero_flags_and_group_rejected(self):
        datagram = build("CHUNK_HAVE", (b"\x01" * 32, 0))
        for index in (4, 5):
            with self.assertRaises(ValueError):
                pd.check_datagram(patch_header(datagram, index, 1), show_output=False)
        with self.assertRaises(ValueError):
            pd.make_datagram("CHUNK_HAVE", 1, 2, pd.PROTOCOL_VERSION, 1, 0, 0, (b"\x01" * 32, 0))

    def test_sequence_fields_only_for_chat_and_ack(self):
        payloads = {
            "CHUNK_REQUEST": (b"\x01" * 32, 0, 0, 960),
            "CHUNK_DATA": (b"\x01" * 32, 0, 0, 10, b"0123456789"),
            "FILE_LIST": [(b"\x01" * 32, 10, 5, "a.bin")],
            "CHUNK_HAVE": (b"\x01" * 32, 0),
            "REPLICATE_REQ": (b"\x01" * 32, 0),
        }
        for name, payload in payloads.items():
            with self.subTest(name=name):
                with self.assertRaises(ValueError):
                    build(name, payload, seq_no=4)
                good = build(name, payload)
                with self.assertRaises(ValueError):
                    pd.check_datagram(patch_header(good, 6, 4), show_output=False)

    def test_old_protocol_version_rejected(self):
        datagram = patch_header(build("PEER_LIST_REQ", None), 3, 1)
        with self.assertRaises(ValueError):
            pd.check_datagram(datagram, show_output=False)


class ChatSessionTests(unittest.TestCase):
    """FIX 5: one-sided peer removal no longer breaks ordered chat."""

    def setUp(self):
        p2p_file.peer_id = 500
        for table in (
            p2p_file.next_packet_sequence_by_peer,
            p2p_file.next_chat_sequence_by_peer,
            p2p_file.expected_packet_sequence_by_peer,
            p2p_file.expected_chat_sequence_by_peer,
            p2p_file.received_chat_pairs_by_peer,
            p2p_file.outgoing_session_by_peer,
            p2p_file.receive_session_by_peer,
            p2p_file.retired_sessions_by_peer,
        ):
            table.clear()

    def test_normal_order_duplicate_and_gap(self):
        classify = p2p_file.classify_incoming_chat
        self.assertEqual(classify(7, 11, 1, 1), "accepted")
        self.assertEqual(classify(7, 11, 1, 1), "duplicate")
        self.assertEqual(classify(7, 11, 3, 3), "reject")
        self.assertEqual(classify(7, 11, 2, 2), "accepted")
        self.assertEqual(classify(7, 11, 3, 3), "accepted")

    def test_receiver_side_removal_keeps_stream(self):
        classify = p2p_file.classify_incoming_chat
        for sequence in range(1, 6):
            self.assertEqual(classify(7, 11, sequence, sequence), "accepted")
        # The receiver times out the sender; the sender kept its state.
        p2p_file.clear_peer_sequence_state(7)
        self.assertEqual(classify(7, 11, 6, 6), "accepted")

    def test_sender_restart_with_new_session_is_displayed(self):
        classify = p2p_file.classify_incoming_chat
        for sequence in range(1, 6):
            self.assertEqual(classify(7, 11, sequence, sequence), "accepted")
        # Old code: (1, 1) was treated as a duplicate and silently dropped.
        self.assertEqual(classify(7, 99, 1, 1), "accepted")
        self.assertEqual(classify(7, 99, 2, 2), "accepted")
        # A delayed datagram from the abandoned session is ignored.
        self.assertEqual(classify(7, 11, 6, 6), "reject")

    def test_sender_gets_new_session_after_removing_peer(self):
        first = p2p_file.outgoing_chat_session(8)
        self.assertEqual(p2p_file.next_packet_sequence(8), 1)
        self.assertEqual(p2p_file.next_packet_sequence(8), 2)
        p2p_file.clear_peer_sequence_state(8)
        self.assertEqual(p2p_file.next_packet_sequence(8), 1)
        second = p2p_file.outgoing_chat_session(8)
        self.assertNotEqual(first, second)


class PlacementTests(unittest.TestCase):
    """FIX 8: bounded HRW placement terminates and never errors oddly."""

    def check(self, chunk_count, peer_ids):
        file_id = bytes(range(32))
        placement = p2p_file.plan_chunk_owners(file_id, chunk_count, peer_ids)
        self.assertEqual(set(placement), set(range(chunk_count)))
        for owners in placement.values():
            self.assertEqual(len(set(owners)), 2)
        for peer in peer_ids:
            self.assertFalse(all(peer in owners for owners in placement.values()))

    def test_four_and_five_peer_layouts(self):
        for chunk_count in (3, 8, 9, 40):
            for peers in ((1, 2, 3, 4), (10, 20, 30, 40, 50)):
                self.check(chunk_count, peers)

    def test_three_peers_three_or_more_chunks(self):
        for chunk_count in range(3, 20):
            self.check(chunk_count, (5, 6, 7))

    def test_three_peers_two_chunks_is_impossible(self):
        # Any two 2-of-3 owner pairs share a peer, which would hold the whole
        # file. The old code looped or raised StopIteration here.
        with self.assertRaises(ValueError):
            p2p_file.plan_chunk_owners(bytes(32), 2, (1, 2, 3))


class FakeSocket:
    """Records datagrams instead of sending them."""

    def __init__(self):
        self.sent = []

    def sendto(self, datagram, addr):
        self.sent.append((datagram, addr))


class FailoverTests(unittest.TestCase):
    """FIX 1: a silent owner must not stop the request to the next owner."""

    def setUp(self):
        import tempfile
        self.store = tempfile.mkdtemp(prefix="p2p_ut_")
        self.saved = (
            getattr(p2p_file, "server", None),
            p2p_file.CHUNK_STORE_DIR,
            list(p2p_file.peers),
            dict(p2p_file.file_catalog_state),
        )
        p2p_file.server = FakeSocket()
        p2p_file.CHUNK_STORE_DIR = self.store
        p2p_file.peer_id = 900
        self.file_id = bytes(range(32))
        self.owner_a = (901, ("127.0.0.1", 49001))
        self.owner_b = (902, ("127.0.0.1", 49002))
        with p2p_file.peer_lock:
            p2p_file.peers[:] = [self.owner_a, self.owner_b]
        with p2p_file.rtt_lock:
            for owner_id, _ in (self.owner_a, self.owner_b):
                p2p_file.peer_rtt_state[owner_id] = {"srtt": 0.1, "rttvar": 0.0, "rto": 0.2}
        catalog = p2p_file.file_catalog_state
        p2p_file.file_catalog_helpers.add_file(catalog, self.file_id, 2000, 1000, "f.bin")
        for index in (0, 1):
            p2p_file.file_catalog_helpers.set_chunk_hash(catalog, self.file_id, index, b"\x00" * 32)
            for owner_id, _ in (self.owner_a, self.owner_b):
                p2p_file.file_catalog_helpers.add_chunk_owner(catalog, self.file_id, index, owner_id)

    def tearDown(self):
        import shutil
        server, store, peers, catalog = self.saved
        p2p_file.server = server
        p2p_file.CHUNK_STORE_DIR = store
        with p2p_file.peer_lock:
            p2p_file.peers[:] = peers
        p2p_file.file_catalog_state.clear()
        p2p_file.file_catalog_state.update(catalog)
        shutil.rmtree(self.store, ignore_errors=True)

    def test_second_owner_is_asked_after_first_stays_silent(self):
        with self.assertRaises(TimeoutError):
            p2p_file.request_chunk(
                self.file_id,
                0,
                owner_order=(self.owner_a[0], self.owner_b[0]),
            )
        request_type = pd.get_packet_type("CHUNK_REQUEST")
        asked = {
            addr for datagram, addr in p2p_file.server.sent
            if datagram[4] == request_type
        }
        # Before FIX 1 only owner A was ever asked.
        self.assertIn(self.owner_a[1], asked)
        self.assertIn(self.owner_b[1], asked)


if __name__ == "__main__":
    unittest.main()
