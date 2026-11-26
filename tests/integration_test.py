"""Live four-peer test over real UDP sockets on this computer.

It starts four peer processes, checks group chat, indexes two 2.5 MB files,
verifies chunk placement (every chunk on >= 2 peers, no peer holds a full
file), downloads a file from several sources, and then kills (crashes) a
source peer in the middle of a second download to check failover.

Run from the project folder:  python tests/integration_test.py
"""

import hashlib
import csv
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
sys.path.insert(0, PROJECT_DIR)
import p2p_file  # noqa: E402


class Peer:
    def __init__(
        self, port, bootstrap, work_dir, store_dir,
        drop_first_chunk=False, force_busy_first_request=False,
    ):
        args = [sys.executable, "-u", PEER_SCRIPT, str(port)]
        if bootstrap:
            args.append(bootstrap)
        environment = dict(
            os.environ,
            P2P_STORE_DIR=store_dir,
            PYTHONUTF8="1",
            P2P_TEST_TRACE="1",
        )
        if drop_first_chunk:
            environment["P2P_TEST_DROP_FIRST_CHUNK_DATA"] = "1"
        if force_busy_first_request:
            environment["P2P_TEST_BUSY_FIRST_CHUNK_REQUEST"] = "1"
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
    generated_size = 0
    while generated_size < FILE_SIZE:
        block = hashlib.sha256(data + counter.to_bytes(8, "big")).digest() * 64
        blocks.append(block)
        generated_size += len(block)
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


def wait_for_active_source(peers, output_starts, downloader_id, file_hex, timeout=20):
    """Return the source that accepted a request during this download."""
    pattern = re.compile(
        rf"P2P_TEST_SERVE_REQUEST:{downloader_id}:{file_hex}:\d+"
    )
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for peer in peers:
            for line in peer.output()[output_starts[peer]:]:
                match = pattern.search(line)
                if match:
                    return peer, match
        time.sleep(0.02)
    raise AssertionError(
        f"no peer accepted a chunk request from downloader {downloader_id} "
        f"for file {file_hex} within {timeout}s"
    )


def get_catalog_owners(peer, file_hex, chunk_count):
    start = len(peer.output())
    peer.send("/files")
    peer.wait_for(rf"file_id={file_hex}", 10, start)
    deadline = time.monotonic() + 10
    owners = {}
    pattern = re.compile(r"chunk (\d+): peers \(([^)]*)\)")
    current_file = None
    while time.monotonic() < deadline:
        for line in peer.output()[start:]:
            file_match = re.search(r"file_id=([0-9a-f]{64})", line)
            if file_match:
                current_file = file_match.group(1)
                continue
            if current_file != file_hex:
                continue
            match = pattern.search(line)
            if match:
                owners[int(match.group(1))] = tuple(
                    int(value.strip())
                    for value in match.group(2).split(",")
                    if value.strip()
                )
        if len(owners) >= chunk_count:
            return owners
        time.sleep(0.1)
    raise AssertionError(f"peer {peer.port} did not receive all chunk owners for {file_hex}")


def wait_for_newcomer_chunks(peer, expected_chunks, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if all(
            chunk_index in peer.chunk_indices(file_hex)
            for file_hex, chunk_index in expected_chunks
        ):
            return True
        time.sleep(0.25)
    return False


def report_measurements(measurements):
    print("\nLocalhost measurements (demonstration values, not a benchmark):")
    for name, value, unit in measurements:
        print(f"  {name}: {value:.3f} {unit}")
    output_dir = os.path.join(PROJECT_DIR, "output")
    os.makedirs(output_dir, exist_ok=True)
    report_path = os.path.join(output_dir, "integration_metrics.csv")
    with open(report_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(("measurement", "value", "unit", "environment"))
        writer.writerows(
            (name, f"{value:.6f}", unit, "localhost")
            for name, value, unit in measurements
        )
    print(f"  saved measurements: {report_path}")


def has_overlapping_chunk_workers(lines, file_hex):
    intervals = {}
    pattern = re.compile(
        rf"P2P_TEST_CHUNK_WORKER_(START|END):{file_hex}:(\d+):([0-9.]+)"
    )
    for line in lines:
        match = pattern.search(line)
        if not match:
            continue
        event, chunk_index, timestamp = match.groups()
        chunk_index = int(chunk_index)
        interval = intervals.setdefault(chunk_index, {})
        interval[event.lower()] = float(timestamp)
    completed = [
        (entry["start"], entry["end"])
        for entry in intervals.values()
        if "start" in entry and "end" in entry
    ]
    return any(
        max(first_start, second_start) < min(first_end, second_end)
        for index, (first_start, first_end) in enumerate(completed)
        for second_start, second_end in completed[index + 1:]
    )


def main():
    work_dir = tempfile.mkdtemp(prefix="p2p_it_")
    store_dir = os.path.join(work_dir, "stores")
    peers = []
    measurements = []
    file_entries = []
    try:
        print("Starting four peers ...")
        peers.append(Peer(
            BASE_PORT, None, work_dir, store_dir,
            drop_first_chunk=True, force_busy_first_request=True,
        ))
        peers[0].wait_for(r"Peer is running", 10)
        for offset in (1, 2, 3):
            peers.append(
                Peer(BASE_PORT + offset, f"localhost:{BASE_PORT}", work_dir, store_dir)
            )
            peers[-1].wait_for(r"Updated peer list", 10)
        time.sleep(5)  # let heartbeats spread the full membership view

        print("Checking group chat ...")
        chat_started = time.monotonic()
        peers[1].send("hello from peer two")
        for peer in (peers[0], peers[2], peers[3]):
            peer.wait_for(r"Chat from peer \d+ .*hello from peer two", 10)
        measurements.append(("chat broadcast delivery", time.monotonic() - chat_started, "seconds"))
        check(True, "chat reached the three other peers")
        for peer in peers:
            displayed = [line for line in peer.output() if "hello from peer two" in line]
            check(len(displayed) <= 1, f"peer {peer.port} displayed the chat at most once")

        print("Checking every peer has the full membership view ...")
        for sender_index, sender in enumerate(peers):
            text = f"membership check from peer {sender_index + 1}"
            sender.send(text)
            for recipient in peers:
                if recipient is not sender:
                    recipient.wait_for(
                        rf"Chat from peer \d+ .*{re.escape(text)}",
                        10,
                    )
        check(True, "all four peers can reach every other peer")

        print("Indexing two 2.5 MB files from each of the four initial peers ...")
        chunk_count = (FILE_SIZE + CHUNK_SIZE - 1) // CHUNK_SIZE
        for peer_number, indexer in enumerate(peers, start=1):
            for file_number in (1, 2):
                name = f"peer{peer_number}_shared{file_number}.bin"
                path = os.path.join(work_dir, name)
                expected_hash = make_file(path, name.encode())
                start = len(indexer.output())
                indexer.send(f'/index "{path}"')
                match = indexer.wait_for(r"file ID ([0-9a-f]{64})", 30, start)
                file_hex = match.group(1)
                check(file_hex == expected_hash, f"{name} is >= 2 MB and identified by its SHA-256")
                wait_for_placement(peers, file_hex, chunk_count, timeout=90)
                file_entries.append((file_hex, indexer))
                check(True, f"{name}: each chunk has >= 2 owners; no peer has the whole file")

        dropped = any(
            line.startswith("P2P_TEST_DROPPED_CHUNK_DATA:")
            for line in peers[0].output()
        )
        retransmitted = any(
            line.startswith("P2P_TEST_CHUNK_RETRANSMIT:")
            for line in peers[0].output()
        )
        check(dropped and retransmitted, "a deliberately dropped CHUNK_DATA fragment was retransmitted")
        forced_busy = any(
            line.startswith("P2P_TEST_FORCED_BUSY:")
            for line in peers[0].output()
        )
        check(forced_busy, "a simulated BUSY reply was exercised during replication")

        print("Checking multi-source download ...")
        file_hashes = [file_entries[0][0], file_entries[2][0]]
        downloader = peers[3]
        start = len(downloader.output())
        download_started = time.monotonic()
        downloader.send(f"/download {file_hashes[0]} out1.bin")
        downloader.wait_for(r"Downloaded and verified file", 60, start)
        download_seconds = time.monotonic() - download_started
        measurements.append(("2.5 MB multi-source download", download_seconds, "seconds"))
        measurements.append(("download throughput", FILE_SIZE / (1024 * 1024) / download_seconds, "MiB/s"))
        with open(os.path.join(work_dir, "out1.bin"), "rb") as handle:
            check(hashlib.sha256(handle.read()).hexdigest() == file_hashes[0], "file 1 rebuilt correctly")
        source_lines = [
            line for line in downloader.output()[start:]
            if line.startswith("P2P_TEST_CHUNK_SOURCE:")
        ]
        source_ids = {
            int(line.split(":", 2)[1]) for line in source_lines
        }
        check(
            len(source_ids) >= 2,
            f"file 1 was actually downloaded from {len(source_ids)} source peers",
        )
        check(
            has_overlapping_chunk_workers(downloader.output()[start:], file_hashes[0]),
            "at least two chunk downloads overlapped in time",
        )
        check(
            len(downloader.chunk_indices(file_hashes[0])) < chunk_count,
            "downloader did not become a full-file owner",
        )

        print("Checking corrupt-chunk rejection and fallback ...")
        corrupt_file_hex = file_entries[0][0]
        owners = get_catalog_owners(downloader, corrupt_file_hex, chunk_count)
        downloader_id = int(downloader.wait_for(r"P2P_TEST_ASSIGNED_ID:(\d+)", 10).group(1))
        peer_by_id = {
            int(peer.wait_for(r"P2P_TEST_ASSIGNED_ID:(\d+)", 10).group(1)): peer
            for peer in peers
        }
        downloader_chunks = downloader.chunk_indices(corrupt_file_hex)
        target_chunk = next(
            index for index in range(chunk_count)
            if index not in downloader_chunks
            and len([owner for owner in owners.get(index, ()) if owner != downloader_id]) >= 2
        )
        corrupt_owner_id = next(
            owner for owner in owners[target_chunk]
            if owner != downloader_id and owner in peer_by_id
        )
        corrupt_owner = peer_by_id[corrupt_owner_id]
        corrupt_path = os.path.join(
            corrupt_owner.store_dir, corrupt_file_hex, f"{target_chunk:08x}.chunk"
        )
        with open(corrupt_path, "rb") as handle:
            original_chunk = handle.read()
        with open(corrupt_path, "wb") as handle:
            handle.write(bytes([original_chunk[0] ^ 1]) + original_chunk[1:])
        downloader.send(f"/test-prefer-owner {corrupt_owner_id}")
        downloader.wait_for(rf"P2P_TEST_PREFERRED_OWNER_SET:{corrupt_owner_id}", 5)
        corrupt_download_start = len(downloader.output())
        corrupt_started = time.monotonic()
        try:
            downloader.send(f"/download {corrupt_file_hex} corrupt-fallback.bin")
            downloader.wait_for(
                rf"P2P_TEST_HASH_MISMATCH:{corrupt_file_hex}:{target_chunk}:{corrupt_owner_id}",
                30,
                corrupt_download_start,
            )
            downloader.wait_for(r"Downloaded and verified file", 60, corrupt_download_start)
            with open(os.path.join(work_dir, "corrupt-fallback.bin"), "rb") as handle:
                check(
                    hashlib.sha256(handle.read()).hexdigest() == corrupt_file_hex,
                    "corrupt replica was rejected and a valid replica rebuilt the file",
                )
        finally:
            with open(corrupt_path, "wb") as handle:
                handle.write(original_chunk)
        measurements.append(("corrupt-replica fallback", time.monotonic() - corrupt_started, "seconds"))

        print("Checking a peer joining after files are shared ...")
        late_joiner = Peer(BASE_PORT + 4, f"localhost:{BASE_PORT}", work_dir, store_dir)
        peers.append(late_joiner)
        late_joiner.wait_for(r"Updated peer list", 15)
        late_joiner.wait_for(r"Received FILE_LIST from peer", 20)
        newcomer_id = int(late_joiner.wait_for(r"P2P_TEST_ASSIGNED_ID:(\d+)", 10).group(1))
        late_joiner.send("/files")
        for file_hex, _ in file_entries:
            late_joiner.wait_for(rf"file_id={file_hex}", 20)
        expected_new_chunks = set()
        joined_ids = [
            int(peer.wait_for(r"P2P_TEST_ASSIGNED_ID:(\d+)", 10).group(1))
            for peer in peers
        ]
        for file_hex, _ in file_entries:
            placement = p2p_file.plan_chunk_owners(
                bytes.fromhex(file_hex), chunk_count, joined_ids
            )
            expected_new_chunks.update(
                (file_hex, chunk_index)
                for chunk_index, owner_ids in placement.items()
                if newcomer_id in owner_ids
            )
        check(
            bool(expected_new_chunks)
            and wait_for_newcomer_chunks(late_joiner, expected_new_chunks),
            "late joiner received every chunk assigned to it in the new placement",
        )
        late_joiner.send(f"new peer {newcomer_id} can chat")
        for peer in peers[:-1]:
            peer.wait_for(rf"Chat from peer {newcomer_id} .*new peer {newcomer_id} can chat", 10)
        check(True, "late joiner can send group chat to existing peers")

        print("Indexing two 2.5 MB files from the late-joining peer ...")
        for file_number in (1, 2):
            name = f"late_peer_shared{file_number}.bin"
            path = os.path.join(work_dir, name)
            expected_hash = make_file(path, name.encode())
            start = len(late_joiner.output())
            late_joiner.send(f'/index "{path}"')
            file_hex = late_joiner.wait_for(r"file ID ([0-9a-f]{64})", 30, start).group(1)
            check(file_hex == expected_hash, f"{name} is >= 2 MB and identified by its SHA-256")
            wait_for_placement(peers, file_hex, chunk_count, timeout=90)
            file_entries.append((file_hex, late_joiner))
            check(True, f"{name}: each chunk has >= 2 owners; no peer has the whole file")

        print("Crashing a source during a download ...")
        downloader = peers[2]
        downloader_id = int(
            downloader.wait_for(r"P2P_TEST_ASSIGNED_ID:(\d+)", 10).group(1)
        )
        file_hex = file_hashes[1]
        source_peers = [peer for peer in peers if peer is not downloader]
        source_output_starts = {
            peer: len(peer.output()) for peer in source_peers
        }
        start = len(downloader.output())
        downloader.send(f"/download {file_hex} out2.bin")
        download_started = time.monotonic()
        victim, _ = wait_for_active_source(
            source_peers,
            source_output_starts,
            downloader_id,
            file_hex,
        )
        victim.stop()
        finished_before_crash = any(
            "Downloaded and verified file" in line
            for line in downloader.output()[start:]
        )
        print(f"  killed peer on port {victim.port} (held {len(victim.chunk_indices(file_hex))} chunks)")
        check(not finished_before_crash, "the source crashed while the download was still running")
        downloader.wait_for(r"Downloaded and verified file", 90, start)
        recovery_seconds = time.monotonic() - download_started
        measurements.append(("source-crash download recovery", recovery_seconds, "seconds"))
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

        report_measurements(measurements)
        print("\nLAN CHECK NOT RUN: this integration test uses localhost; repeat the demo on four separate machines.")
        print("\nINTEGRATION TEST PASSED")
        return 0
    except AssertionError as error:
        print("\nINTEGRATION TEST FAILED:", error)
        for peer in peers:
            print(f"\n--- recent output from peer on port {peer.port} ---")
            for line in peer.output()[-25:]:
                print(line)
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
