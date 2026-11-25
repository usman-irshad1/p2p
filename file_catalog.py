"""In-memory file metadata and chunk ownership helpers for the P2P project.

This module does not send UDP packets or read/write chunk contents. It keeps
the catalog state that p2p_file.py can use when FILE_LIST and CHUNK_HAVE
messages are connected to the live peer program.

Catalog shape:
{
    file_id_bytes: {
        "file_size": int,
        "chunk_size": int,
        "filename": str,
        "chunk_count": int,
        "chunk_hashes": {chunk_index: bytes},
        "chunk_owners": {chunk_index: {peer_id, ...}},
    }
}
"""

import os

import protocol_design


MAX_PEER_ID = 65534  # 65535 is reserved for broadcast.
MAX_CHUNK_INDEX = 0xFFFFFFFF
MAX_FILE_SIZE = 0xFFFFFFFFFFFFFFFF
MAX_CHUNK_SIZE = 0xFFFFFFFF


def create_catalog():
    """Return an empty in-memory catalog."""
    return {}


def get_chunk_count(file_size, chunk_size):
    """Return how many fixed-size chunks are needed for a file."""
    if not isinstance(file_size, int) or isinstance(file_size, bool):
        raise ValueError("file_size must be an integer")
    if not isinstance(chunk_size, int) or isinstance(chunk_size, bool):
        raise ValueError("chunk_size must be an integer")
    if not 0 <= file_size <= MAX_FILE_SIZE:
        raise ValueError("file_size must fit in an unsigned 64-bit field")
    if not 1 <= chunk_size <= MAX_CHUNK_SIZE:
        raise ValueError("chunk_size must fit in a nonzero unsigned 32-bit field")

    return (file_size + chunk_size - 1) // chunk_size


def _validate_file_id(file_id):
    if not isinstance(file_id, bytes) or len(file_id) != 32:
        raise ValueError("file_id must be a 32-byte SHA-256 digest")


def _validate_filename(filename):
    if not isinstance(filename, str) or not filename:
        raise ValueError("filename must be a nonempty string")
    if len(filename.encode("utf-8")) > 65535:
        raise ValueError("filename is too long for the FILE_LIST format")


def add_file(catalog, file_id, file_size, chunk_size, filename):
    """Add file metadata with an initially unknown owner for each chunk.

    Re-adding identical metadata is safe and preserves already learned owners.
    Conflicting metadata for the same file ID is rejected.
    """
    _validate_file_id(file_id)
    _validate_filename(filename)
    chunk_count = get_chunk_count(file_size, chunk_size)
    if chunk_count > MAX_CHUNK_INDEX + 1:
        raise ValueError("file has more chunks than the protocol can index")

    existing = catalog.get(file_id)
    if existing is not None:
        expected = (existing["file_size"], existing["chunk_size"], existing["filename"])
        received = (file_size, chunk_size, filename)
        if expected != received:
            raise ValueError("Conflicting metadata for this file ID")
        return file_id

    catalog[file_id] = {
        "file_size": file_size,
        "chunk_size": chunk_size,
        "filename": filename,
        "chunk_count": chunk_count,
        "chunk_hashes": {},
        "chunk_owners": {
            chunk_index: set()
            for chunk_index in range(chunk_count)
        },
    }
    return file_id


def set_chunk_hash(catalog, file_id, chunk_index, chunk_hash):
    """Record one expected chunk digest, rejecting conflicting values."""
    _validate_file_id(file_id)
    if not isinstance(chunk_index, int) or isinstance(chunk_index, bool):
        raise ValueError("chunk_index must be an integer")
    if not isinstance(chunk_hash, bytes) or len(chunk_hash) != 32:
        raise ValueError("chunk_hash must be a 32-byte SHA-256 digest")
    metadata = catalog.get(file_id)
    if metadata is None:
        raise KeyError("file metadata must be added before its chunk hash")
    if chunk_index not in metadata["chunk_owners"]:
        raise ValueError("chunk_index is outside this file's chunk range")

    hashes = metadata["chunk_hashes"]
    existing_hash = hashes.get(chunk_index)
    if existing_hash is not None and existing_hash != chunk_hash:
        raise ValueError("Conflicting hash for this file chunk")
    hashes[chunk_index] = chunk_hash
    return existing_hash is None


def register_file(catalog, file_path, chunk_size):
    """Hash a local file and add its metadata; this does not assign chunks."""
    file_id = protocol_design.get_file_id(file_path)
    file_size = os.path.getsize(file_path)
    filename = os.path.basename(os.fspath(file_path))
    add_file(catalog, file_id, file_size, chunk_size, filename)
    return file_id


def merge_file_list(catalog, file_records):
    """Merge decoded FILE_LIST records into the local catalog.

    Each record uses protocol_design's format:
    (file_id, file_size, chunk_size, filename).
    Existing chunk-owner information is kept.
    """
    merged_ids = []
    for file_id, file_size, chunk_size, filename in file_records:
        merged_ids.append(
            add_file(catalog, file_id, file_size, chunk_size, filename)
        )
    return tuple(merged_ids)


def file_list_records(catalog):
    """Return sorted metadata records ready for a FILE_LIST payload."""
    records = []
    for file_id, metadata in catalog.items():
        records.append((
            file_id,
            metadata["file_size"],
            metadata["chunk_size"],
            metadata["filename"],
        ))
    return tuple(sorted(records, key=lambda record: record[0]))


def add_chunk_owner(catalog, file_id, chunk_index, peer_id):
    """Record that a peer currently stores one chunk; return whether it was new."""
    _validate_file_id(file_id)
    if file_id not in catalog:
        raise KeyError("file metadata must be added before chunk ownership")
    if not isinstance(peer_id, int) or isinstance(peer_id, bool):
        raise ValueError("peer_id must be an integer")
    if not 1 <= peer_id <= MAX_PEER_ID:
        raise ValueError("peer_id must be 1..65534; 65535 is broadcast")

    chunks = catalog[file_id]["chunk_owners"]
    if not isinstance(chunk_index, int) or isinstance(chunk_index, bool):
        raise ValueError("chunk_index must be an integer")
    if chunk_index not in chunks:
        raise ValueError("chunk_index is outside this file's chunk range")

    owners = chunks[chunk_index]
    was_new = peer_id not in owners
    owners.add(peer_id)
    return was_new


def remove_chunk_owner(catalog, file_id, chunk_index, peer_id):
    """Forget one peer's ownership claim; return True if it was present."""
    _validate_file_id(file_id)
    metadata = catalog.get(file_id)
    if metadata is None or chunk_index not in metadata["chunk_owners"]:
        return False

    owners = metadata["chunk_owners"][chunk_index]
    if peer_id not in owners:
        return False
    owners.remove(peer_id)
    return True


def get_chunk_owners(catalog, file_id, chunk_index):
    """Return the known peer IDs for a chunk in ascending order."""
    _validate_file_id(file_id)
    metadata = catalog.get(file_id)
    if metadata is None:
        raise KeyError("file is not in the catalog")
    if chunk_index not in metadata["chunk_owners"]:
        raise ValueError("chunk_index is outside this file's chunk range")
    return tuple(sorted(metadata["chunk_owners"][chunk_index]))


def remove_peer(catalog, peer_id):
    """Remove a departed peer and return chunks that now have no known owner."""
    orphaned_chunks = []
    for file_id, metadata in catalog.items():
        for chunk_index, owners in metadata["chunk_owners"].items():
            if peer_id in owners:
                owners.remove(peer_id)
                if not owners:
                    orphaned_chunks.append((file_id, chunk_index))
    return tuple(orphaned_chunks)


def under_replicated_chunks(catalog, minimum_replicas=2):
    """Return chunk locations with fewer than the requested number of owners."""
    if not isinstance(minimum_replicas, int) or isinstance(minimum_replicas, bool):
        raise ValueError("minimum_replicas must be an integer")
    if minimum_replicas < 1:
        raise ValueError("minimum_replicas must be at least one")

    under_replicated = []
    for file_id, metadata in catalog.items():
        for chunk_index, owners in metadata["chunk_owners"].items():
            if len(owners) < minimum_replicas:
                under_replicated.append(
                    (file_id, chunk_index, tuple(sorted(owners)))
                )
    return tuple(under_replicated)
