import struct
import hashlib
import socket

HEADER_FORMAT = "!HHBBHIHHHH"
header_size = struct.calcsize(HEADER_FORMAT)
CHECKSUM_OFFSET = header_size - 2


def checksum(data):
    """Return the RFC 1071 Internet checksum of bytes in network order."""
    if len(data) % 2:
        data = data + b"\x00"

    total = 0
    for index in range(0, len(data), 2):
        total += (data[index] << 8) | data[index + 1]

    while total >> 16:
        total = (total & 0xFFFF) + (total >> 16)

    return (~total) & 0xFFFF


def compute_checksum(datagram):
    """Cover the application header and payload with the checksum field zeroed.

    No IP/UDP pseudo-header is included. Odd-length padding is used only for
    calculation and is never transmitted.
    """
    if len(datagram) < header_size:
        raise ValueError("Datagram is shorter than the header")

    zeroed = (datagram[:CHECKSUM_OFFSET] + b"\x00\x00"
              + datagram[CHECKSUM_OFFSET + 2:])
    return checksum(zeroed)


def _with_checksum(datagram):
    value = compute_checksum(datagram)
    return (datagram[:CHECKSUM_OFFSET] + struct.pack("!H", value)
            + datagram[CHECKSUM_OFFSET + 2:])

def check_datagram(datagram):
    if len(datagram) < header_size:
        raise ValueError("Datagram is shorter than the header")

    decoded_fields = struct.unpack(HEADER_FORMAT, datagram[:header_size])
    if compute_checksum(datagram) != decoded_fields[9]:
        raise ValueError("Checksum mismatch")

    actual_payload_length = len(datagram[header_size:])
    packet_type = decoded_fields[2]
    payload = datagram[header_size:]

    if actual_payload_length == decoded_fields[8]:
        if packet_type == 1:
            print(struct.unpack(HEADER_FORMAT, datagram[:header_size]))
            chat_sequence = struct.unpack("!I", payload[:4])[0]
            message_delivered = payload[4:].decode("utf-8")
            print("Chat sequence:", chat_sequence)
            print(message_delivered)

        elif packet_type == 11:  # NEW_PEER
            if len(payload) != 8:
                raise ValueError("NEW_PEER payload must be exactly 8 bytes")

            known_id, ip_bytes, port = struct.unpack("!H4sH", payload)
            ip = socket.inet_ntoa(ip_bytes)
            print("New peer:", known_id, ip, port)
            return known_id, ip, port

        elif packet_type == 2 or packet_type == 9:
            if len(payload) == 36:
                file_id = payload[:32]
                chunk_index = struct.unpack("!I", payload[-4:])[0]
                print("file_id:", file_id.hex())
                print("chunk_index:", chunk_index)
            else:
                print("The packet length is invalid")

        elif packet_type == 0 or packet_type == 3 or packet_type == 4:
            print("No payload")

        elif packet_type == 5:
            if len(payload) == 0:
                ack_sequence_number = decoded_fields[7]
                print(ack_sequence_number)
            else:
                print("The packet is invalid")

        elif packet_type == 6:  # JOIN_ACK
            if len(payload) < 2:
                raise ValueError("JOIN_ACK payload is missing the peer count")

            peer_count = struct.unpack("!H", payload[:2])[0]
            expected_length = 2 + peer_count * 8

            if len(payload) != expected_length:
                raise ValueError(
                    "JOIN_ACK payload length does not match peer count"
                )

            peers_in_ack = []

            for index in range(peer_count):
                start = 2 + index * 8
                end = start + 8

                known_id, ip_bytes, port = struct.unpack(
                    "!H4sH", payload[start:end]
                )
                ip = socket.inet_ntoa(ip_bytes)
                peers_in_ack.append((known_id, ip, port))

            print("Acknowledged JOIN sequence:", decoded_fields[7])
            print("Peers in JOIN_ACK:", peers_in_ack)

        elif packet_type == 7:  # CHUNK_DATA
            if len(payload) < 44:
                raise ValueError("CHUNK_DATA payload is missing metadata")

            file_id, chunk_index, fragment_offset, total_chunk_size = (
                struct.unpack("!32sIII", payload[:44])
            )
            fragment_data = payload[44:]

            if fragment_offset + len(fragment_data) > total_chunk_size:
                raise ValueError("Fragment extends beyond the chunk")

            print("file_id:", file_id.hex())
            print("chunk_index:", chunk_index)
            print("fragment_offset:", fragment_offset)
            print("total_chunk_size:", total_chunk_size)
            print("fragment bytes:", len(fragment_data))

            return (
                file_id,
                chunk_index,
                fragment_offset,
                total_chunk_size,
                fragment_data,
            )

        elif packet_type == 8:  # FILE_LIST
            if len(payload) < 2:
                raise ValueError("FILE_LIST is missing the file count")

            file_count = struct.unpack("!H", payload[:2])[0]
            files = []
            position = 2
            record_size = struct.calcsize("!32sQIH")

            for _ in range(file_count):
                if len(payload) - position < record_size:
                    raise ValueError(
                        "FILE_LIST has an incomplete file record"
                    )

                file_id, file_size, chunk_size, filename_length = struct.unpack(
                    "!32sQIH",
                    payload[position:position + record_size],
                )
                position += record_size

                filename_end = position + filename_length

                if filename_end > len(payload):
                    raise ValueError("FILE_LIST has an incomplete filename")

                filename = payload[position:filename_end].decode("utf-8")
                position = filename_end

                files.append(
                    (file_id, file_size, chunk_size, filename)
                )

            if position != len(payload):
                raise ValueError("FILE_LIST contains unexpected extra bytes")

            print("Files in FILE_LIST:")
            for file_id, file_size, chunk_size, filename in files:
                print(file_id.hex(), file_size, chunk_size, filename)

            return files

        elif packet_type == 10:  # REPLICATE_REQ
            if len(payload) != 36:
                raise ValueError(
                    "REPLICATE_REQ payload must be exactly 36 bytes"
                )

            file_id = payload[:32]
            chunk_index = struct.unpack("!I", payload[32:])[0]

            print("REPLICATE_REQ file_id:", file_id.hex())
            print("REPLICATE_REQ chunk_index:", chunk_index)

            return file_id, chunk_index



    else:
        print("corruption in the payload")


def reassemble_chunk(fragments):
    fragments.sort(key=lambda part: part[2])

    total_size = fragments[0][3]
    chunk = bytearray()
    expected_offset = 0

    for file_id, chunk_index, offset, part_total_size, data in fragments:
        if part_total_size != total_size:
            raise ValueError("Fragments disagree about total chunk size")

        if offset != expected_offset:
            raise ValueError("A fragment is missing or overlaps another")

        chunk.extend(data)
        expected_offset += len(data)

    if expected_offset != total_size:
        raise ValueError("Chunk is incomplete")

    return bytes(chunk)


def get_packet_type(packet_name):
    packet_name = packet_name.upper()

    if packet_name == "JOIN":
        return 0
    elif packet_name == "CHAT":
        return 1
    elif packet_name == "CHUNK_REQUEST":
        return 2
    elif packet_name == "LEAVE":
        return 3
    elif packet_name == "HEARTBEAT":
        return 4
    elif packet_name == "ACK":
        return 5
    elif packet_name == "JOIN_ACK":
        return 6
    elif packet_name == "CHUNK_DATA":
        return 7
    elif packet_name == "FILE_LIST":
        return 8
    elif packet_name == "CHUNK_HAVE":
        return 9
    elif packet_name == "REPLICATE_REQ":
        return 10
    elif packet_name == "NEW_PEER":
        return 11
    else:
        raise ValueError("Unknown packet type")


def get_file_id(file_path):
    file_hash = hashlib.sha256()

    with open(file_path, "rb") as file:
        for block in iter(lambda: file.read(65536), b""):
            file_hash.update(block)

    return file_hash.digest()


def make_payload(payload, packet_type):
    if packet_type == 0 or packet_type == 3 or packet_type == 4:
        return b""

    elif packet_type == 1:
        chat_sequence, text = payload
        return struct.pack("!I", chat_sequence) + text.encode("utf-8")

    elif packet_type == 2:
        file_id, chunk_index = payload

        if len(file_id) != 32:
            raise ValueError("file_id must be a 32-byte SHA-256 hash")

        return file_id + struct.pack("!I", chunk_index)

    elif packet_type == 5:  # ACK
        return b""

    elif packet_type == 6:  # JOIN_ACK
        peer_list = payload
        payload_bytes = struct.pack("!H", len(peer_list))

        for known_id, ip, port in peer_list:
            peer_record = struct.pack(
                "!H4sH",
                known_id,
                socket.inet_aton(ip),
                port,
            )
            payload_bytes += peer_record

        return payload_bytes

    elif packet_type == 7:  # CHUNK_DATA
        file_id, chunk_index, fragment_offset, total_chunk_size, fragment_data = payload

        if len(file_id) != 32:
            raise ValueError("file_id must be 32 bytes")

        if not isinstance(fragment_data, bytes):
            raise ValueError("fragment_data must be bytes")

        if fragment_offset + len(fragment_data) > total_chunk_size:
            raise ValueError("Fragment extends beyond the chunk")

        metadata = struct.pack(
            "!32sIII",
            file_id,
            chunk_index,
            fragment_offset,
            total_chunk_size,
        )

        return metadata + fragment_data

    elif packet_type == 8:  # FILE_LIST
        files = payload

        if len(files) > 65535:
            raise ValueError("Too many files for one FILE_LIST packet")

        payload_bytes = struct.pack("!H", len(files))

        for file_id, file_size, chunk_size, filename in files:
            if not isinstance(file_id, bytes) or len(file_id) != 32:
                raise ValueError("file_id must be 32 bytes")

            if not isinstance(filename, str):
                raise ValueError("filename must be a string")

            if file_size < 0:
                raise ValueError("file_size cannot be negative")

            if chunk_size <= 0:
                raise ValueError("chunk_size must be greater than zero")

            filename_bytes = filename.encode("utf-8")

            if len(filename_bytes) > 65535:
                raise ValueError("filename is too long")

            file_record = struct.pack(
                "!32sQIH",
                file_id,
                file_size,
                chunk_size,
                len(filename_bytes),
            )

            payload_bytes += file_record + filename_bytes

        return payload_bytes

    elif packet_type == 9:  # CHUNK_HAVE
        file_id, chunk_index = payload

        if len(file_id) != 32:
            raise ValueError("file_id must be a 32-byte SHA-256 hash")

        return file_id + struct.pack("!I", chunk_index)

    elif packet_type == 11:  # NEW_PEER
        known_id, ip, port = payload
        return struct.pack(
            "!H4sH",
            known_id,
            socket.inet_aton(ip),
            port,
        )

    elif packet_type == 10:  # REPLICATE_REQ
        file_id, chunk_index = payload

        if not isinstance(file_id, bytes) or len(file_id) != 32:
            raise ValueError("file_id must be 32 bytes")

        return file_id + struct.pack("!I", chunk_index)

    else:
        raise NotImplementedError(
            "Payload format not added for this packet type"
        )
    


def make_datagram(
    packet_name,
    peer_id,
    destination_p_id,
    protocol_version,
    flags,
    group_id,
    seq_no,
    payload,
    checksum=0,
    ack_no=None,
):
    """Build a datagram and calculate its checksum automatically.

    The legacy checksum argument is accepted for existing callers but ignored;
    the checksum always reflects the final header and payload bytes.
    """
    packet_type = get_packet_type(packet_name)
    payload_encoded = make_payload(payload, packet_type)

    if packet_type == 0:  # JOIN
        fields = (
            peer_id,
            destination_p_id,
            packet_type,
            protocol_version,
            flags,
            group_id,
            seq_no,
            ack_no,
            len(payload_encoded),
            checksum,
        )
        header_bytes = struct.pack(HEADER_FORMAT, *fields)
        return _with_checksum(header_bytes)

    if packet_type == 1:  # CHAT
        fields = (
            peer_id,
            destination_p_id,
            packet_type,
            protocol_version,
            flags,
            group_id,
            seq_no,
            ack_no,
            len(payload_encoded),
            checksum,
        )
        header_bytes = struct.pack(HEADER_FORMAT, *fields)
        return _with_checksum(header_bytes + payload_encoded)

    if packet_type == 2:  # CHUNK_REQUEST
        fields = (
            peer_id,
            destination_p_id,
            packet_type,
            protocol_version,
            flags,
            group_id,
            seq_no,
            ack_no,
            len(payload_encoded),
            checksum,
        )
        header_bytes = struct.pack(HEADER_FORMAT, *fields)
        return _with_checksum(header_bytes + payload_encoded)

    if packet_type == 3:  # LEAVE
        fields = (
            peer_id,
            destination_p_id,
            packet_type,
            protocol_version,
            flags,
            group_id,
            seq_no,
            ack_no,
            len(payload_encoded),
            checksum,
        )
        header_bytes = struct.pack(HEADER_FORMAT, *fields)
        return _with_checksum(header_bytes + payload_encoded)

    if packet_type == 4:  # HEARTBEAT
        fields = (
            peer_id,
            destination_p_id,
            packet_type,
            protocol_version,
            flags,
            group_id,
            seq_no,
            ack_no,
            len(payload_encoded),
            checksum,
        )
        header_bytes = struct.pack(HEADER_FORMAT, *fields)
        return _with_checksum(header_bytes + payload_encoded)

    if packet_type == 5:  # ACK
        if ack_no is None:
            raise ValueError(
                "ACK requires the sequence number being acknowledged"
            )

        fields = (
            peer_id,
            destination_p_id,
            packet_type,
            protocol_version,
            flags,
            group_id,
            seq_no,
            ack_no,
            0,
            checksum,
        )
        return _with_checksum(struct.pack(HEADER_FORMAT, *fields))

    if packet_type == 6:  # JOIN_ACK
        if ack_no is None:
            raise ValueError("JOIN_ACK requires the JOIN sequence number")

        fields = (
            peer_id,
            destination_p_id,
            packet_type,
            protocol_version,
            flags,
            group_id,
            seq_no,
            ack_no,
            len(payload_encoded),
            checksum,
        )
        header_bytes = struct.pack(HEADER_FORMAT, *fields)
        return _with_checksum(header_bytes + payload_encoded)

    if packet_type == 7:  # CHUNK_DATA
        fields = (
            peer_id,
            destination_p_id,
            packet_type,
            protocol_version,
            flags,
            group_id,
            seq_no,
            ack_no,
            len(payload_encoded),
            checksum,
        )
        header_bytes = struct.pack(HEADER_FORMAT, *fields)
        return _with_checksum(header_bytes + payload_encoded)

    if packet_type == 8:  # FILE_LIST
        fields = (
            peer_id,
            destination_p_id,
            packet_type,
            protocol_version,
            flags,
            group_id,
            seq_no,
            ack_no,
            len(payload_encoded),
            checksum,
        )
        header_bytes = struct.pack(HEADER_FORMAT, *fields)
        return _with_checksum(header_bytes + payload_encoded)

    if packet_type == 9:  # CHUNK_HAVE
        fields = (
            peer_id,
            destination_p_id,
            packet_type,
            protocol_version,
            flags,
            group_id,
            seq_no,
            ack_no,
            len(payload_encoded),
            checksum,
        )
        header_bytes = struct.pack(HEADER_FORMAT, *fields)
        return _with_checksum(header_bytes + payload_encoded)

    if packet_type == 10:  # REPLICATE_REQ
        fields = (
            peer_id,
            destination_p_id,
            packet_type,
            protocol_version,
            flags,
            group_id,
            seq_no,
            ack_no,
            len(payload_encoded),
            checksum,
        )
        header_bytes = struct.pack(HEADER_FORMAT, *fields)
        return _with_checksum(header_bytes + payload_encoded)

    if packet_type == 11:  # NEW_PEER
        fields = (
            peer_id,
            destination_p_id,
            packet_type,
            protocol_version,
            flags,
            group_id,
            seq_no,
            ack_no,
            len(payload_encoded),
            checksum,
        )
        header_bytes = struct.pack(HEADER_FORMAT, *fields)
        return _with_checksum(header_bytes + payload_encoded)



def run_checksum_tests():
    """Run checksum regression tests alongside the existing packet demos."""
    import contextlib
    import io
    import unittest

    class ChecksumTests(unittest.TestCase):
        def packet(self, name="CHAT", payload=(1, "hello"), **overrides):
            fields = dict(packet_name=name, peer_id=1001,
                          destination_p_id=1002, protocol_version=1, flags=0,
                          group_id=0, seq_no=1, ack_no=0, payload=payload)
            fields.update(overrides)
            return make_datagram(**fields)

        def test_known_vectors(self):
            # RFC 1071 section 3 example: sum DDF2, complement 220D.
            self.assertEqual(checksum(bytes.fromhex("0001f203f4f5f6f7")), 0x220D)
            self.assertEqual(checksum(b""), 0xFFFF)
            self.assertEqual(checksum(b"\x01"), 0xFEFF)
            self.assertEqual(checksum(b"\xff" * 5), 0x00FF)
            self.assertEqual(checksum(b"\xff\xff"), 0)

        def test_all_packet_types(self):
            cases = [
                ("JOIN", None), ("CHAT", (1, "hello")),
                ("CHUNK_REQUEST", (b"a" * 32, 2)), ("LEAVE", None),
                ("HEARTBEAT", None), ("ACK", None),
                ("JOIN_ACK", [(1003, "127.0.0.1", 5000)]),
                ("CHUNK_DATA", (b"a" * 32, 2, 0, 3, b"abc")),
                ("FILE_LIST", [(b"a" * 32, 3, 3, "a.txt")]),
                ("CHUNK_HAVE", (b"a" * 32, 2)),
                ("REPLICATE_REQ", (b"a" * 32, 2)),
                ("NEW_PEER", (1003, "127.0.0.1", 5000)),
            ]
            for name, payload in cases:
                with self.subTest(packet=name):
                    packet = self.packet(name, payload)
                    stored = struct.unpack_from("!H", packet, CHECKSUM_OFFSET)[0]
                    self.assertEqual(stored, compute_checksum(packet))
                    self.assertEqual(checksum(packet), 0)
                    with contextlib.redirect_stdout(io.StringIO()):
                        check_datagram(packet)

        def test_odd_and_even_payloads(self):
            for text in ("ab", "abc", "", "سلام"):
                with self.subTest(text=text):
                    packet = self.packet(payload=(1, text))
                    self.assertEqual(len(packet), header_size + 4 + len(text.encode()))
                    self.assertEqual(checksum(packet), 0)

        def test_header_corruption_rejected(self):
            packet = bytearray(self.packet())
            packet[0] ^= 1
            with self.assertRaisesRegex(ValueError, "Checksum mismatch"):
                check_datagram(bytes(packet))

        def test_payload_corruption_rejected(self):
            packet = bytearray(self.packet())
            packet[-1] ^= 1
            with self.assertRaisesRegex(ValueError, "Checksum mismatch"):
                check_datagram(bytes(packet))

        def test_checksum_corruption_rejected(self):
            packet = bytearray(self.packet())
            packet[CHECKSUM_OFFSET] ^= 1
            with self.assertRaisesRegex(ValueError, "Checksum mismatch"):
                check_datagram(bytes(packet))

        def test_short_datagrams_rejected(self):
            for length in (0, 1, header_size - 1):
                for function in (compute_checksum, check_datagram):
                    with self.subTest(length=length, function=function.__name__):
                        with self.assertRaisesRegex(ValueError, "shorter than the header"):
                            function(bytes(length))

        def test_existing_checksum_is_zeroed(self):
            packet = self.packet()
            modified = packet[:CHECKSUM_OFFSET] + b"\x12\x34" + packet[header_size:]
            self.assertEqual(compute_checksum(packet), compute_checksum(modified))
            self.assertEqual(self.packet(checksum=1234), packet)

        def test_zero_checksum_is_valid(self):
            # Header word sum before checksum is FFFF, so its complement is zero.
            packet = self.packet("JOIN", None, peer_id=0xFFFE,
                                 destination_p_id=0, seq_no=0)
            self.assertEqual(packet[CHECKSUM_OFFSET:header_size], b"\x00\x00")
            with contextlib.redirect_stdout(io.StringIO()):
                check_datagram(packet)

    suite = unittest.defaultTestLoader.loadTestsFromTestCase(ChecksumTests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    if not result.wasSuccessful():
        raise SystemExit(1)


def main():
    run_checksum_tests()

    # Test ACK.
    ack_packet = make_datagram(
        packet_name="ACK",
        peer_id=1002,
        destination_p_id=1001,
        protocol_version=1,
        flags=0,
        group_id=0,
        seq_no=1,
        ack_no=12,
        payload=None,
        checksum=0,
    )

    print("Testing ACK:")
    check_datagram(ack_packet)

    ack_fields = struct.unpack(HEADER_FORMAT, ack_packet[:header_size])
    assert ack_fields[2] == 5
    assert ack_fields[7] == 12
    assert ack_fields[8] == 0

    # Test JOIN_ACK with two peers.
    peer_list = [
        (1001, "127.0.0.1", 5000),
        (1003, "192.168.1.20", 5002),
    ]

    join_ack_packet = make_datagram(
        packet_name="JOIN_ACK",
        peer_id=1001,
        destination_p_id=1004,
        protocol_version=1,
        flags=0,
        group_id=0,
        seq_no=2,
        ack_no=7,
        payload=peer_list,
        checksum=0,
    )

    print("\nTesting JOIN_ACK:")
    check_datagram(join_ack_packet)

    join_ack_fields = struct.unpack(
        HEADER_FORMAT,
        join_ack_packet[:header_size],
    )
    join_ack_payload = join_ack_packet[header_size:]
    peer_count = struct.unpack("!H", join_ack_payload[:2])[0]

    assert join_ack_fields[2] == 6
    assert join_ack_fields[7] == 7
    assert peer_count == 2
    assert join_ack_fields[8] == 2 + (2 * 8)

    # Test NEW_PEER.
    new_peer_info = (1004, "192.168.1.25", 5004)

    new_peer_packet = make_datagram(
        packet_name="NEW_PEER",
        peer_id=1001,
        destination_p_id=1002,
        protocol_version=1,
        flags=0,
        group_id=0,
        seq_no=3,
        ack_no=0,
        payload=new_peer_info,
        checksum=0,
    )

    print("\nTesting NEW_PEER:")
    decoded_new_peer = check_datagram(new_peer_packet)

    new_peer_fields = struct.unpack(
        HEADER_FORMAT,
        new_peer_packet[:header_size],
    )

    assert new_peer_fields[2] == 11
    assert new_peer_fields[8] == 8
    assert decoded_new_peer == new_peer_info

    # Test FILE_LIST with two files.
    files_to_send = [
        (b"\x01" * 32, 1024, 512, "notes.txt"),
        (b"\x02" * 32, 2500, 1000, "photo.jpg"),
    ]

    file_list_packet = make_datagram(
        packet_name="FILE_LIST",
        peer_id=1001,
        destination_p_id=1002,
        protocol_version=1,
        flags=0,
        group_id=0,
        seq_no=4,
        ack_no=0,
        payload=files_to_send,
        checksum=0,
    )

    print("\nTesting FILE_LIST:")
    decoded_files = check_datagram(file_list_packet)

    assert decoded_files == files_to_send
    print("FILE_LIST test passed.")

    print("\nACK, JOIN_ACK, NEW_PEER, and FILE_LIST tests passed.")

    print("testing the replicate acks")
    requested_chunk = (b"\x03" * 32, 6)

    replicate_packet = make_datagram(
        packet_name="REPLICATE_REQ",
        peer_id=1001,
        destination_p_id=1002,
        protocol_version=1,
        flags=0,
        group_id=0,
        seq_no=5,
        ack_no=0,
        payload=requested_chunk,
        checksum=0,
    )

    decoded_request = check_datagram(replicate_packet)

    fields = struct.unpack(HEADER_FORMAT, replicate_packet[:header_size])
    assert fields[2] == 10
    assert fields[8] == 36
    assert decoded_request == requested_chunk

    print("REPLICATE_REQ test passed.")


if __name__ == "__main__":
    main()
