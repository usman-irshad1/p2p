"""Live four-peer test over real UDP sockets on this computer.

It starts four peer processes, checks group chat, indexes two 2.5 MB files,
verifies chunk placement (every chunk on >= 2 peers, no peer holds a full
file), downloads a file from several sources, and then kills (crashes) a
source peer in the middle of a second download to check failover.

Run from the project folder:  python tests/integration_test.py
"""

import hashlib
import os
import queue
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PEER_SCRIPT = os.path.join(PROJECT_DIR, "p2p_file.py")
BASE_PORT = int(os.environ.get("P2P_TEST_PORT", "47100"))
FILE_SIZE = 2_500_000
CHUNK_SIZE = 256 * 1024


class Peer:
    def __init__(self, port, bootstrap, work_dir, store_dir):
        args = [sys.executable, "-u", PEER_SCRIPT, str(port)]
        if bootstrap:
            args.append(bootstrap)
        environment = dict(os.environ, P2P_STORE_DIR=store_dir, PYTHONUTF8="1")
        self.port = port
        self.store_dir = os.path.join(store_dir, str(port))
        self.lines = []
        self.lines_lock = threading.Lock()
        self.new_lines = queue.Queue()
        self.process = subprocess.Popen(
            args,
            cwd=work_dir,
            env=environment,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            bufsize=1,
        )
        threading.Thread(target=self._read, daemon=True).start()

    def _read(self):
        for line in self.process.stdout:
            line = line.rstrip("\n")
            with self.lines_lock:
                self.lines.append(line)

    def send(self, command):
        self.process.stdin.write(command + "\n")
        self.process.stdin.flush()

    def output(self):
        with self.lines_lock:
            return list(self.lines)

    def wait_for(self, pattern, timeout, start=0):
        deadline = time.monotonic() + timeout
        regex = re.compile(pattern)
        while time.monotonic() < deadline:
            for line in self.output()[start:]:
                match = regex.search(line)
                if match:
                    return match
            if self.process.poll() is not None:
                break
            time.sleep(0.1)
        raise AssertionError(
            f"peer {self.port}: no line matching {pattern!r} within {timeout}s\n"
            + "\n".join(self.output()[-40:])
        )

    def chunk_indices(self, file_hex):
        folder = os.path.join(self.store_dir, file_hex)
        if not os.path.isdir(folder):
            return set()
        return {
            int(name.split(".")[0], 16)
            for name in os.listdir(folder)
            if name.endswith(".chunk")
        }

    def stop(self):
        if self.process.poll() is None:
            self.process.kill()
        self.process.wait(timeout=5)


def make_file(path, seed):
    data = hashlib.sha256(seed).digest()
    blocks = []
    counter = 0
    while sum(len(block) for block in blocks) < FILE_SIZE:
        blocks.append(hashlib.sha256(data + counter.to_bytes(8, "big")).digest() * 64)
        counter += 1
    content = b"".join(blocks)[:FILE_SIZE]
    with open(path, "wb") as handle:
        handle.write(content)
    return hashlib.sha256(content).hexdigest()


def wait_for_placement(peers, file_hex, chunk_count, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        holdings = {peer.port: peer.chunk_indices(file_hex) for peer in peers}
        copies = {
            index: sum(index in held for held in holdings.values())
            for index in range(chunk_count)
        }
        full = [port for port, held in holdings.items() if len(held) == chunk_count]
        if min(copies.values()) >= 2 and not full:
            return holdings
        time.sleep(0.5)
    raise AssertionError(f"placement not reached: copies={copies}, full={full}")


def check(condition, message):
    if not condition:
        raise AssertionError(message)
    print("  PASS", message)


def main():
    work_dir = tempfile.mkdtemp(prefix="p2p_it_")
    store_dir = os.path.join(work_dir, "stores")
    peers = []
    try:
        print("Starting four peers ...")
        peers.append(Peer(BASE_PORT, None, work_dir, store_dir))
        peers[0].wait_for(r"Peer is running", 10)
        for offset in (1, 2, 3):
            peers.append(
                Peer(BASE_PORT + offset, f"localhost:{BASE_PORT}", work_dir, store_dir)
            )
            peers[-1].wait_for(r"Updated peer list", 10)
        time.sleep(5)  # let heartbeats spread the full membership view

        print("Checking group chat ...")
        peers[1].send("hello from peer two")
        for peer in (peers[0], peers[2], peers[3]):
            peer.wait_for(r"Chat from peer \d+ .*hello from peer two", 10)
        check(True, "chat reached the three other peers")
        for peer in peers:
            displayed = [line for line in peer.output() if "hello from peer two" in line]
            check(len(displayed) <= 1, f"peer {peer.port} displayed the chat at most once")

        print("Indexing two 2.5 MB files ...")
        chunk_count = (FILE_SIZE + CHUNK_SIZE - 1) // CHUNK_SIZE
        file_paths = []
        file_hashes = []
        for number, indexer in ((1, peers[0]), (2, peers[1])):
            path = os.path.join(work_dir, f"shared{number}.bin")
            file_hashes.append(make_file(path, f"file-{number}".encode()))
            file_paths.append(path)
            start = len(indexer.output())
            indexer.send(f'/index "{path}"')
            match = indexer.wait_for(r"file ID ([0-9a-f]{64})", 20, start)
            check(match.group(1) == file_hashes[-1], f"file {number} ID is its SHA-256")

        holdings = {}
        for number, file_hex in enumerate(file_hashes, 1):
            holdings[file_hex] = wait_for_placement(peers, file_hex, chunk_count)
            check(True, f"file {number}: every chunk on >= 2 peers, no peer has the whole file")

        print("Downloading file 1 on peer four ...")
        downloader = peers[3]
        start = len(downloader.output())
        downloader.send(f"/download {file_hashes[0]} out1.bin")
        downloader.wait_for(r"Downloaded and verified file", 60, start)
        with open(os.path.join(work_dir, "out1.bin"), "rb") as handle:
            check(hashlib.sha256(handle.read()).hexdigest() == file_hashes[0], "file 1 rebuilt correctly")
        sources = {
            port
            for port, held in holdings[file_hashes[0]].items()
            if held and port != downloader.port
        }
        check(len(sources) >= 2, f"file 1 chunks were spread over {len(sources)} source peers")
        check(
            len(downloader.chunk_indices(file_hashes[0])) < chunk_count,
            "downloader did not become a full-file owner",
        )

        print("Crashing a source during the download of file 2 ...")
        downloader = peers[2]
        file_hex = file_hashes[1]
        victims = [
            peer for peer in peers
            if peer is not downloader and peer.chunk_indices(file_hex)
        ]
        victim = max(victims, key=lambda peer: len(peer.chunk_indices(file_hex)))
        start = len(downloader.output())
        downloader.send(f"/download {file_hex} out2.bin")
        download_started = time.monotonic()
        time.sleep(0.3)
        victim.stop()
        finished_before_crash = any(
            "Downloaded and verified file" in line
            for line in downloader.output()[start:]
        )
        print(f"  killed peer on port {victim.port} (held {len(victim.chunk_indices(file_hex))} chunks)")
        check(not finished_before_crash, "the source crashed while the download was still running")
        downloader.wait_for(r"Downloaded and verified file", 90, start)
        print(f"  download finished {time.monotonic() - download_started:.1f}s after it started")
        with open(os.path.join(work_dir, "out2.bin"), "rb") as handle:
            check(hashlib.sha256(handle.read()).hexdigest() == file_hex, "file 2 rebuilt correctly after a source crash")

        survivors = [peer for peer in peers if peer is not victim]
        for peer in survivors:
            peer.wait_for(rf"timed out and was removed", 20)
        check(True, "survivors detected the crash by heartbeat timeout")

        print("Waiting for re-replication after the crash ...")
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            copies = [
                sum(index in peer.chunk_indices(file_hex) for peer in survivors)
                for index in range(chunk_count)
            ]
            if min(copies) >= 2:
                break
            time.sleep(1)
        check(min(copies) >= 2, "every chunk of file 2 is back on >= 2 live peers")

        print("\nINTEGRATION TEST PASSED")
        return 0
    except AssertionError as error:
        print("\nINTEGRATION TEST FAILED:", error)
        return 1
    finally:
        for peer in peers:
            try:
                peer.stop()
            except Exception:
                pass
        shutil.rmtree(work_dir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
