import struct
import hashlib
HEADER_FORMAT = "!HHBBHIHHHH"
import socket
header_size=struct.calcsize(HEADER_FORMAT)
print(struct.calcsize(HEADER_FORMAT))

def check_datagram(datagram):
    if len(datagram) < header_size:
        raise ValueError("Datagram is shorter than the header")
    decoded_fields = struct.unpack(HEADER_FORMAT, datagram[:header_size])
    actual_payload_length = len(datagram[header_size:])
    packet_type=decoded_fields[2]
    payload = datagram[header_size:]

    if (actual_payload_length == decoded_fields[8]):
        if( packet_type==1):
            print(struct.unpack(HEADER_FORMAT,datagram[:header_size]))
            chat_sequence = struct.unpack("!I", payload[:4])[0]
            message_delvered = payload[4:].decode("utf-8")
            print("Chat sequence:", chat_sequence)
            print(message_delvered)

        elif packet_type == 2 or packet_type == 9: 
            if (len(payload)==36):
                file_id = payload[:32]                            # first 32 bytes
                chunk_index = struct.unpack("!I", payload[-4:])[0]  # last 4 bytes
                print("file_id:", file_id.hex())
                print("chunk_index:", chunk_index)
            else:
                print("The packet length is invalid")

        elif packet_type == 0 or packet_type == 3 or packet_type == 4:
            print("No payload") 

        elif packet_type == 5: 
                    if (len(payload)==0):
                        ack_sequce_number=decoded_fields[7]
                        print(ack_sequce_number)   
                    else:
                        print("The packet is invalid")
           


        elif packet_type == 7:  # CHUNK_DATA
            if len(payload) < 44:
                raise ValueError("CHUNK_DATA payload is missing metadata")

            file_id, chunk_index, fragment_offset, total_chunk_size = struct.unpack( "!32sIII", payload[:44])
            fragment_data = payload[44:]

            if fragment_offset + len(fragment_data) > total_chunk_size:
                raise ValueError("Fragment extends beyond the chunk")

            print("file_id:", file_id.hex())
            print("chunk_index:", chunk_index)
            print("fragment_offset:", fragment_offset)
            print("total_chunk_size:", total_chunk_size)
            print("fragment bytes:", len(fragment_data))
            return file_id, chunk_index, fragment_offset, total_chunk_size, fragment_data
    else:
        print("corruption in the payload")




def reassemble_chunk(fragments):
    fragments.sort(key=lambda part: part[2])  # sort by fragment_offset

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
    elif packet_name == "NEW PEER":
        return 11
    else:
        raise ValueError("Unknown packet type")



def get_file_id(file_path):
    file_hash = hashlib.sha256()

    with open(file_path, "rb") as file:
        for block in iter(lambda: file.read(65536), b""):
            file_hash.update(block)

    return file_hash.digest()  # 32 bytes


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

        for known_id, ip, port in peer_list:  # this is for the peer list to be shared to other peers when a new peer joins
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

    elif packet_type == 9:
        file_id, chunk_index = payload
        if len(file_id) != 32:
            raise ValueError("file_id must be a 32-byte SHA-256 hash")

        return file_id + struct.pack("!I", chunk_index)

    else:
        raise NotImplementedError("Payload format not added for this packet type")
     


def make_datagram(packet_name, peer_id, destination_p_id, protocol_version,flags,group_id,seq_no, payload, checksum,ack_no=None):
    packet_type = get_packet_type(packet_name)
    
    payload_encoded = make_payload(payload,packet_type)
    if (packet_type==0):# for join
        fields = (
        peer_id,          # source peer ID
        destination_p_id,          # destination peer ID
        packet_type,             # packet type, e.g. JOIN
        protocol_version,             # protocol version
        flags,             # flags
        group_id,             # group ID
        seq_no,             # sequence number
        ack_no,             # acknowledgment number
        len(payload_encoded),  # payload length
        checksum,             # checksum (not calculated yet)
        )
        header_bytes = struct.pack(HEADER_FORMAT, *fields)
        data_frame=header_bytes
        return data_frame

    if (packet_type==1):# for chat
        fields = (
        peer_id,          # source peer ID
        destination_p_id,          # destination peer ID
        packet_type,             # packet type, e.g. JOIN
        protocol_version,             # protocol version
        flags,             # flags
        group_id,             # group ID
        seq_no,             # sequence number
        ack_no,             # acknowledgment number
        len(payload_encoded),  # payload length
        checksum,             # checksum (not calculated yet)
    )
        header_bytes = struct.pack(HEADER_FORMAT, *fields)
        data_frame=header_bytes+payload_encoded
        return data_frame

    if (packet_type==2):# for CHUNK_REQUEST
        fields = (
        peer_id,          # source peer ID
        destination_p_id,          # destination peer ID
        packet_type,             # packet type, e.g. JOIN
        protocol_version,             # protocol version
        flags,             # flags
        group_id,             # group ID
        seq_no,             # sequence number
        ack_no,             # acknowledgment number
        len(payload_encoded),  # payload length
        checksum,             # checksum (not calculated yet)
    )
        header_bytes = struct.pack(HEADER_FORMAT, *fields)
        data_frame=header_bytes+payload_encoded
        return data_frame

    if (packet_type==3):# for leave
            fields = (
            peer_id,          # source peer ID
            destination_p_id,          # destination peer ID
            packet_type,             # packet type, e.g. JOIN
            protocol_version,             # protocol version
            flags,             # flags
            group_id,             # group ID
            seq_no,             # sequence number
            ack_no,             # acknowledgment number
            len(payload_encoded),  # payload length
            checksum,             # checksum (not calculated yet)
        )
            header_bytes = struct.pack(HEADER_FORMAT, *fields)
            data_frame=header_bytes+payload_encoded
            return data_frame

    if (packet_type==4):# for heartbeat
            fields = (
            peer_id,          # source peer ID
            destination_p_id,          # destination peer ID
            packet_type,             # packet type, e.g. JOIN
            protocol_version,             # protocol version
            flags,             # flags
            group_id,             # group ID
            seq_no,             # sequence number
            ack_no,             # acknowledgment number
            len(payload_encoded),  # payload length
            checksum,             # checksum (not calculated yet)
        )
            header_bytes = struct.pack(HEADER_FORMAT, *fields)
            data_frame=header_bytes+payload_encoded
            return data_frame

    if (packet_type==5):# for ack
            if ack_no is None:
                raise ValueError("ACK requires the sequence number being acknowledged")
            fields = (
            peer_id,          # source peer ID
            destination_p_id,          # destination peer ID
            packet_type,             # packet type, e.g. JOIN
            protocol_version,             # protocol version
            flags,             # flags
            group_id,             # group ID
            seq_no,             # sequence number
            ack_no,             # acknowledgment number
            0,  # payload length
            checksum,             # checksum (not calculated yet)
        )
            header_bytes = struct.pack(HEADER_FORMAT, *fields)
            data_frame=header_bytes
            return data_frame

    if (packet_type==6):# for join ack
            if ack_no is None:
                raise ValueError("JOIN_ACK requires the JOIN sequence number")
            fields = (
            peer_id,          # source peer ID
            destination_p_id,          # destination peer ID
            packet_type,             # packet type, e.g. JOIN
            protocol_version,             # protocol version
            flags,             # flags
            group_id,             # group ID
            seq_no,             # sequence number
            ack_no,             # acknowledgment number
            len(payload_encoded),  # payload length
            checksum,             # checksum (not calculated yet)
        )
            header_bytes = struct.pack(HEADER_FORMAT, *fields)
            data_frame=header_bytes+payload_encoded
            return data_frame

    if (packet_type==7):# for chuck data
            
            fields = (
            peer_id,          # source peer ID
            destination_p_id,          # destination peer ID
            packet_type,             # packet type, e.g. JOIN
            protocol_version,             # protocol version
            flags,             # flags
            group_id,             # group ID
            seq_no,             # sequence number
            ack_no,             # acknowledgment number
            len(payload_encoded),  # payload length
            checksum,             # checksum (not calculated yet)
        )
            header_bytes = struct.pack(HEADER_FORMAT, *fields)
            data_frame=header_bytes+payload_encoded
            return data_frame

    if (packet_type==8):# for FILE_LIST
            fields = (
            peer_id,          # source peer ID
            destination_p_id,          # destination peer ID
            packet_type,             # packet type, e.g. JOIN
            protocol_version,             # protocol version
            flags,             # flags
            group_id,             # group ID
            seq_no,             # sequence number
            ack_no,             # acknowledgment number
            len(payload_encoded),  # payload length
            checksum,             # checksum (not calculated yet)
        )
            header_bytes = struct.pack(HEADER_FORMAT, *fields)
            data_frame=header_bytes+payload_encoded
            return data_frame

    if (packet_type==9):# for Chunk_Have
            fields = (
            peer_id,          # source peer ID
            destination_p_id,          # destination peer ID
            packet_type,             # packet type, e.g. JOIN
            protocol_version,             # protocol version
            flags,             # flags
            group_id,             # group ID
            seq_no,             # sequence number
            ack_no,             # acknowledgment number
            len(payload_encoded),  # payload length
            checksum,             # checksum (not calculated yet)
        )
            header_bytes = struct.pack(HEADER_FORMAT, *fields)
            data_frame=header_bytes+payload_encoded
            return data_frame

    
    if (packet_type==10):# for replicate_req
            fields = (
            peer_id,          # source peer ID
            destination_p_id,          # destination peer ID
            packet_type,             # packet type, e.g. JOIN
            protocol_version,             # protocol version
            flags,             # flags
            group_id,             # group ID
            seq_no,             # sequence number
            ack_no,             # acknowledgment number
            len(payload_encoded),  # payload length
            checksum,             # checksum (not calculated yet)
        )
            header_bytes = struct.pack(HEADER_FORMAT, *fields)
            data_frame=header_bytes+payload_encoded
            return data_frame






def main():
    file_id = b"\x01" * 32
    chunk_index = 3

    # A pretend 1000-byte chunk
    original_chunk = b"A" * 500 + b"B" * 500
    fragments = []

    # Make two 500-byte CHUNK_DATA packets
    for offset in (0, 500):
        fragment_data = original_chunk[offset:offset + 500]

        packet = make_datagram(
            packet_name="CHUNK_DATA",
            peer_id=1001,
            destination_p_id=1002,
            protocol_version=1,
            flags=0,
            group_id=0,
            seq_no=offset // 500,
            ack_no=0,
            payload=(
                file_id,
                chunk_index,
                offset,
                len(original_chunk),
                fragment_data,
            ),
            checksum=0,
        )

        fragment_info = check_datagram(packet)
        if fragment_info is not None:
            fragments.append(fragment_info)

    rebuilt_chunk = reassemble_chunk(fragments)

    print("Fragments collected:", len(fragments))
    print("Rebuilt size:", len(rebuilt_chunk))
    print("Matches original:", rebuilt_chunk == original_chunk)


if __name__ == "__main__":
    main()
