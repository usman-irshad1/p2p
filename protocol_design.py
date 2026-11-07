import struct
HEADER_FORMAT = "!HHBBHIHHHH"
header_size=struct.calcsize(HEADER_FORMAT)
print(struct.calcsize(HEADER_FORMAT))

def check_datagram(data_gram):
    if len(data_gram) < header_size:
        raise ValueError("Datagram is shorter than the header")
    decoded_fields = struct.unpack(HEADER_FORMAT, data_gram[:header_size])
    actual_payload_length = len(data_gram[header_size:])
    if (actual_payload_length == decoded_fields[8]):
        message_delvered=data_gram[header_size:].decode("utf-8")
        print(struct.unpack(HEADER_FORMAT,data_gram[:header_size]))
        print("Decoded fields:", decoded_fields)
        print(message_delvered)
    else:
        print("corruption in the payload")

def make_datagram(packet_type, peer_id, destination_p_id, protocol_version,flags,group_id,seq_no, ack_no, payload, checksum):
    payload_encoded = (payload.encode("utf-8"))
    if (packet_type==0):# for join
        fields = (
        peer_id,          # source peer ID
        destination_p_id,          # destination peer ID
        packet_type,             # packet type, e.g. JOIN
        protocol_version,             # protocol version
        flags,             # flags
        group_id,             # group ID
        0,             # sequence number
        0,             # acknowledgment number
        0,  # payload length
        0,             # checksum (not calculated yet)
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
    


packet = make_datagram(
    packet_type=1,
    peer_id=1001,
    destination_p_id=1002,
    protocol_version=1,
    flags=0,
    group_id=0,
    seq_no=7,
    ack_no=0,
    payload="hello",
    checksum=0,
)

print("Datagram bytes:", packet)
check_datagram(packet)