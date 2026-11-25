"""Run the project checks with one command.

Usage from the project directory:
    python integration_test.py
"""

import os
import random
import socket
import subprocess
import sys
from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parent


def find_free_port_block(count=5):
    """Find consecutive localhost UDP ports for the integration peers."""
    for _ in range(200):
        first_port = random.randrange(40000, 60000 - count)
        sockets = []
        try:
            for port in range(first_port, first_port + count):
                peer_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                peer_socket.bind(("127.0.0.1", port))
                sockets.append(peer_socket)
            return first_port
        except OSError:
            pass
        finally:
            for peer_socket in sockets:
                peer_socket.close()
    raise RuntimeError("Could not find five free consecutive UDP ports.")


def run_check(command, title, environment):
    print(f"\n=== {title} ===", flush=True)
    result = subprocess.run(command, cwd=PROJECT_DIR, env=environment)
    return result.returncode


def main():
    environment = os.environ.copy()
    environment.setdefault("P2P_TEST_PORT", str(find_free_port_block()))

    unit_result = run_check(
        [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-v"],
        "Protocol and reliability unit checks",
        environment,
    )
    if unit_result != 0:
        return unit_result

    return run_check(
        [sys.executable, "-u", "tests/integration_test.py"],
        "Multi-peer UDP integration check",
        environment,
    )


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, RuntimeError) as error:
        print(f"Integration checks could not start: {error}", file=sys.stderr)
        sys.exit(1)
