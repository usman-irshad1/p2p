import socket
import threading
import queue
import random
import sys
import time

messages = queue.Queue()
peers = []

local_port = int(sys.argv[1])
bootstrap_addr = None

if len(sys.argv) > 2:
    host, port = sys.argv[2].rsplit(":", 1)
    bootstrap_addr = (host, int(port))


advertise_ip = "127.0.0.1"
server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
server.bind(("0.0.0.0", local_port))


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

    for known_id, known_addr in peers:
        if known_id != joining_id:
            entries.append((known_id, known_addr[0], known_addr[1]))

    peer_list_text = ";".join( f"{known_id},{ip},{port}"
        for known_id, ip, port in entries
    )

    message = f"join_ack:{peer_list_text}".encode("utf-8")
    server.sendto(message, joiner_addr)


def join(peer_id, bootstrap_addr):
    message = f"join:{peer_id}".encode("utf-8")
    server.sendto(message, bootstrap_addr)


def broadcast():
    while True:
        message, addr = messages.get()

        try:
            text = message.decode("utf-8")
            print(text)

            if text.startswith("join:"):
                joining_id = int(text.split(":", 1)[1])

                if not any(known_id == joining_id for known_id, _ in peers):
                    peers.append((joining_id, addr))

                
                join_ack(addr, joining_id)

                
                outgoing = f"{joining_id} joined".encode("utf-8")

                for peer in peers:
                    if peer[1] != addr:
                        server.sendto(outgoing, peer[1])

            elif text.startswith("join_ack:"):
                peer_list_text = text.split(":", 1)[1]

                if peer_list_text:
                    for entry in peer_list_text.split(";"):
                        known_id_text, known_ip, known_port_text = entry.split(",")
                        known_id = int(known_id_text)
                        known_addr = (known_ip, int(known_port_text))

                        if known_id != peer_id and not any(
                            existing_id == known_id
                            for existing_id, _ in peers
                        ):
                            peers.append((known_id, known_addr))

                print("Updated peer list:", peers)

        except UnicodeDecodeError:
            print(f"Ignoring invalid message from {addr}")
        finally:
            messages.task_done()


peer_id = id_generation()

threading.Thread(target=receive, daemon=True).start()
threading.Thread(target=broadcast, daemon=True).start()

if bootstrap_addr is not None:
    join(peer_id, bootstrap_addr)
else:
    print("Started as the first peer")

print("Peer is running on port", local_port)

while True:
    time.sleep(1)