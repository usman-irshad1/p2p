import socket
import threading
import queue
import random
import sys
import time

messages = queue.Queue()
peers = []
last_seen = {}
peer_lock = threading.Lock()

HEARTBEAT_INTERVAL = 5
PEER_TIMEOUT = 15


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
    """Remove a peer by ID, optionally requiring its source address to match."""
    with peer_lock:
        for known_id, known_addr in tuple(peers):
            if known_id == peer_id and (
                peer_addr is None or known_addr == peer_addr
            ):
                peers.remove((known_id, known_addr))
                return True

    return False


def send_chat_to_active_peers(chat_datagram):
    """Unicast one chat datagram to each peer in a membership snapshot."""
    recipients = peer_snapshot()

    for known_id, peer_addr in recipients:
        server.sendto(chat_datagram, peer_addr)

    return recipients


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


def broadcast():
    while True:
        message, addr = messages.get()

        try:
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

    peer_id = id_generation()

    threading.Thread(target=receive, daemon=True).start()
    threading.Thread(target=broadcast, daemon=True).start()
    threading.Thread(target=heartbeat, daemon=True).start()

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
