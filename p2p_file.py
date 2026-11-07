import socket
import threading
import queue
import random

messages=queue.Queue() #this is making a FIFO queue that is thread safe
peers=[]

server=socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # creating a UDP socket
server.bind(('192.168.100.84',5000))


def recieve():
    while True :
        try :
            message, addr=  server.recvfrom(1024)
            messages.put((message,addr))
        except:
            pass

def broadcast():
    while True:
        message, addr= messages.get()
        try:
            text = message.decode("utf-8")
            print(text)

            if text.startswith("join:"):
                peer_id = text.split(":", 1)[1]
                outgoing = f"{peer_id} joined".encode("utf-8")

                if addr not in peers:                             
                    peers.append((peer_id,addr))

                for peer in peers:
                    if peer[1] != addr:
                        server.sendto(outgoing, peer[1])
        except UnicodeDecodeError:
            print(f"Ignoring invalid message from {addr}")
        finally:
            messages.task_done()

def id_generation():
    peer_id = random.randrange(1, 65535)
    return peer_id

def join(peer_id):
    message_start=f"join:{peer_id}"
    server.sendto( message_start.encode("utf-8"),('192.168.100.84',5000))
    


threading.Thread(target=recieve, daemon=True).start()
threading.Thread(target=broadcast, daemon=True).start()
peer_id=id_generation()
join(peer_id)

    
print("Server is running...")
import time
while True:
    time.sleep(1)
    