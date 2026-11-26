"""Disk helpers for storing and rebuilding file chunks without classes."""

import hashlib
import os


def _validate_file_id(file_id):
    if not isinstance(file_id, bytes) or len(file_id) != 32:
        raise ValueError("file_id must be a 32-byte SHA-256 digest")


def _validate_chunk_index(chunk_index):
    if not isinstance(chunk_index, int) or isinstance(chunk_index, bool):
        raise ValueError("chunk_index must be an integer")
    if not 0 <= chunk_index <= 0xFFFFFFFF:
        raise ValueError("chunk_index must fit in an unsigned 32-bit field")


def get_chunk_path(storage_dir, file_id, chunk_index):
    """Return the path used for one chunk in the local chunk store."""
    _validate_file_id(file_id)
    _validate_chunk_index(chunk_index)
    return os.path.join(storage_dir, file_id.hex(), f"{chunk_index:08x}.chunk")


def store_chunk(storage_dir, file_id, chunk_index, chunk_data):
    """Write a complete chunk to disk, replacing an older copy if present."""
    if not isinstance(chunk_data, bytes):
        raise ValueError("chunk_data must be bytes")

    destination = get_chunk_path(storage_dir, file_id, chunk_index)
    os.makedirs(os.path.dirname(destination), exist_ok=True)
    temporary = destination + ".tmp"

    with open(temporary, "wb") as chunk_file:
        chunk_file.write(chunk_data)
    os.replace(temporary, destination)
    return destination


def load_chunk(storage_dir, file_id, chunk_index):
    """Read and return a stored chunk's bytes."""
    path = get_chunk_path(storage_dir, file_id, chunk_index)
    with open(path, "rb") as chunk_file:
        return chunk_file.read()


def has_chunk(storage_dir, file_id, chunk_index):
    """Return whether the local store has this chunk file."""
    return os.path.isfile(get_chunk_path(storage_dir, file_id, chunk_index))


def delete_chunk(storage_dir, file_id, chunk_index):
    """Remove one local chunk file; return whether it existed."""
    path = get_chunk_path(storage_dir, file_id, chunk_index)
    if not os.path.isfile(path):
        return False
    os.remove(path)
    return True


def split_and_store_file(source_path, storage_dir, file_id, chunk_size):
    """Split a source file into fixed-size chunks and save each one locally."""
    _validate_file_id(file_id)
    if not isinstance(chunk_size, int) or isinstance(chunk_size, bool):
        raise ValueError("chunk_size must be an integer")
    if not 1 <= chunk_size <= 0xFFFFFFFF:
        raise ValueError("chunk_size must fit in a nonzero uint32")

    chunk_indices = []
    with open(source_path, "rb") as source_file:
        chunks = iter(lambda: source_file.read(chunk_size), b"")
        for chunk_index, chunk_data in enumerate(chunks):
            store_chunk(storage_dir, file_id, chunk_index, chunk_data)
            chunk_indices.append(chunk_index)

    return tuple(chunk_indices)


def assemble_file(storage_dir, file_id, chunk_count, destination_path):
    """Join stored chunks in order and verify the result against the file ID."""
    _validate_file_id(file_id)
    if not isinstance(chunk_count, int) or isinstance(chunk_count, bool):
        raise ValueError("chunk_count must be an integer")
    if chunk_count < 0:
        raise ValueError("chunk_count cannot be negative")

    destination_path = os.fspath(destination_path)
    temporary_path = destination_path + ".part"
    digest = hashlib.sha256()
    destination_directory = os.path.dirname(destination_path)
    if destination_directory:
        os.makedirs(destination_directory, exist_ok=True)

    try:
        with open(temporary_path, "wb") as output_file:
            for chunk_index in range(chunk_count):
                chunk_data = load_chunk(storage_dir, file_id, chunk_index)
                output_file.write(chunk_data)
                digest.update(chunk_data)

        if digest.digest() != file_id:
            raise ValueError("Reassembled file SHA-256 does not match file_id")

        os.replace(temporary_path, destination_path)
    except Exception:
        if os.path.exists(temporary_path):
            os.remove(temporary_path)
        raise

    return destination_path
