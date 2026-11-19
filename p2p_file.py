import socket
import threading
import queue
import random
import sys
import time
import struct
from collections import deque
import protocol_design

messages = queue.Queue()
peers = []
last_seen = {}
peer_lock = threading.Lock()
sequence_lock = threading.Lock()
chat_state_lock = threading.Lock()
outgoing_chat_queues = {}
pending_chats = {}

PACKET_SEQUENCE_MAX = 0xFFFF
CHAT_SEQUENCE_MAX = 0xFFFFFFFF
next_packet_sequence_by_peer = {}
next_chat_sequence_by_peer = {}
expected_packet_sequence_by_peer = {}
expected_chat_sequence_by_peer = {}

HEARTBEAT_INTERVAL = 5
PEER_TIMEOUT = 15
CHAT_RETRY_INTERVAL = 1


def peer_snapshot():
    """Return a stable snapshot of the current peer ID/address pairs."""
    with peer_lock:
        return tuple(peers)


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
        clear_peer_sequence_state(peer_id)
        clear_pending_chat(peer_id)

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


def clear_peer_sequence_state(removed_peer_id):
    """Forget sequence counters in both directions when a peer is removed."""
    with sequence_lock:
        for sequence_map in (
            next_packet_sequence_by_peer,
            next_chat_sequence_by_peer,
            expected_packet_sequence_by_peer,
            expected_chat_sequence_by_peer,
        ):
            for peer_pair in tuple(sequence_map):
                if removed_peer_id in peer_pair:
                    sequence_map.pop(peer_pair, None)


def send_chat_to_active_peers(chat_datagram):
    """Unicast one chat datagram to each peer in a membership snapshot."""
    recipients = peer_snapshot()

    for known_id, peer_addr in recipients:
        server.sendto(chat_datagram, peer_addr)

    return recipients


def queue_group_chat(text):
    """Queue this chat separately for every peer currently in the group."""
    # Validate the UTF-8 byte limit before placing the message in any queue.
    protocol_design.make_chat_payload(1, text)
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
        pending = pending_chats.pop(removed_peer_id, None)

    if pending is not None or queue_for_peer:
        print(f"Chat delivery to peer {removed_peer_id} failed; peer was removed")


def chat_sender():
    """Send one CHAT at a time per peer and retry it unchanged until ACKed."""
    while True:
        now = time.monotonic()

        for destination_id, destination_addr in peer_snapshot():
            datagram_to_send = None

            with chat_state_lock:
                pending = pending_chats.get(destination_id)

                if pending is not None and pending["acknowledged"]:
                    pending_chats.pop(destination_id, None)
                    pending = None

                if pending is not None:
                    if now - pending["sent_at"] >= CHAT_RETRY_INTERVAL:
                        pending["sent_at"] = now
                        datagram_to_send = pending["datagram"]
                else:
                    queue_for_peer = outgoing_chat_queues.get(destination_id)
                    if queue_for_peer:
                        text = queue_for_peer.popleft()
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
                            payload=(chat_sequence, text),
                        )
                        pending_chats[destination_id] = {
                            "seq_no": packet_sequence,
                            "datagram": datagram_to_send,
                            "addr": destination_addr,
                            "sent_at": now,
                            "acknowledged": False,
                        }

            if datagram_to_send is not None:
                server.sendto(datagram_to_send, destination_addr)

        time.sleep(0.05)


def chat_input():
    """Read chat lines from the terminal and queue them for active peers."""
    while True:
        try:
            text = input()
            if text:
                queue_group_chat(text)
        except (ValueError, UnicodeError) as error:
            print(f"Cannot queue chat message: {error}")
        except EOFError:
            return


def receive():
    while True:
        try:
            message, addr = server.recvfrom(1024)
            messages.put((message, addr))
        except:
            pass


def id_generation():
    return random.randrange(1, 65535)


def join_ack(joiner_addr, joining_id):
    entries = [(peer_id, advertise_ip, local_port)]

    for known_id, known_addr in peer_snapshot():
        if known_id != joining_id:
            entries.append((known_id, known_addr[0], known_addr[1]))

    peer_list_text = ";".join(
        f"{known_id},{ip},{port}"
        for known_id, ip, port in entries
    )

    message = f"join_ack:{peer_list_text}".encode("utf-8")
    server.sendto(message, joiner_addr)


def join(peer_id, bootstrap_addr):
    message = f"join:{peer_id}".encode("utf-8")
    server.sendto(message, bootstrap_addr)


def leave(peer_id):
    message = f"leave:{peer_id}".encode("utf-8") #this to notify every peer that the peer is leavig so that they can remove the peer from their peer list
    for known_id, peer_addr in peer_snapshot():
        server.sendto(message, peer_addr)


def heartbeat():
    while True:
        message = f"heartbeat:{peer_id}".encode("utf-8")

        for known_id, peer_addr in peer_snapshot():
            server.sendto(message, peer_addr)

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

        time.sleep(HEARTBEAT_INTERVAL)


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
            ack_datagram = protocol_design.make_ack_for_chat(message, peer_id)
            chat_sequence, chat_text = protocol_design.decode_chat_payload(
                message[protocol_design.header_size:]
            )
            sender_id = struct.unpack_from("!H", message, 0)[0]
            server.sendto(ack_datagram, addr)
            print(f"Chat from peer {sender_id} (sequence {chat_sequence}): {chat_text}")
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
                    pending = pending_chats.get(sender_id)
                    if (
                        pending is not None
                        and pending["addr"] == addr
                        and pending["seq_no"] == ack_no
                    ):
                        pending["acknowledged"] = True
    except ValueError as error:
        print(f"Ignoring invalid protocol packet from {addr}: {error}")

    return True


def broadcast():
    while True:
        message, addr = messages.get()

        try:
            if handle_protocol_chat_or_ack(message, addr):
                continue

            text = message.decode("utf-8")
            print(text)

            if text.startswith("join:"):
                joining_id = int(text.split(":", 1)[1])

                if add_peer(joining_id, addr):
                    last_seen[joining_id] = time.monotonic()

                join_ack(addr, joining_id)

                outgoing = (
                    f"new_peer:{joining_id},{addr[0]},{addr[1]}"
                ).encode("utf-8")

                for known_id, known_addr in peer_snapshot():
                    if known_id != joining_id:
                        server.sendto(outgoing, known_addr)

            elif text.startswith("join_ack:"):
                peer_list_text = text.split(":", 1)[1]

                if peer_list_text:
                    for entry in peer_list_text.split(";"):
                        known_id_text, known_ip, known_port_text = entry.split(",")
                        known_id = int(known_id_text)
                        known_addr = (known_ip, int(known_port_text))

                        if known_id != peer_id and add_peer(
                            known_id, known_addr
                        ):
                            last_seen[known_id] = time.monotonic()

                print("Updated peer list:", peer_snapshot())

            elif text.startswith("new_peer:"):
                peer_info = text.split(":", 1)[1]
                known_id_text, known_ip, known_port_text = peer_info.split(",")

                known_id = int(known_id_text)
                known_addr = (known_ip, int(known_port_text))

                if known_id != peer_id and add_peer(known_id, known_addr):
                    last_seen[known_id] = time.monotonic()

                print("Updated peer list:", peer_snapshot())

            elif text.startswith("heartbeat:"):
                sender_id = int(text.split(":", 1)[1])

                for known_id, known_addr in peer_snapshot():
                    if known_id == sender_id and known_addr == addr:
                        last_seen[sender_id] = time.monotonic()
                        break


            elif text.startswith("leave:"):
                leaving_id=int(text.split(":", 1)[1])
                if remove_peer(leaving_id, addr):
                    last_seen.pop(leaving_id, None)

                print("Updated peer list:", peer_snapshot())

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


def main():
    global server, local_port, bootstrap_addr
    global advertise_ip, peer_id

    local_port = int(sys.argv[1])
    bootstrap_addr = None

    if len(sys.argv) > 2:
        host, port = sys.argv[2].rsplit(":", 1)
        bootstrap_addr = (host, int(port))

    advertise_ip = "127.0.0.1"

    server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server.bind(("0.0.0.0", local_port))
    server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server.bind(("0.0.0.0", local_port))

    test_peer_helpers()

    peer_id = id_generation()


    threading.Thread(target=receive, daemon=True).start()
    threading.Thread(target=broadcast, daemon=True).start()
    threading.Thread(target=heartbeat, daemon=True).start()
    threading.Thread(target=chat_sender, daemon=True).start()
    threading.Thread(target=chat_input, daemon=True).start()

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
