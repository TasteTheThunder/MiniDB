"""Functional dependency persistence utilities."""

import json
import os

FD_SUFFIX = ".fds.json"
FD_META_DIR = "metadata"


def _fd_file_path(table_name):
    """Return FD file path for a table."""
    return os.path.join(FD_META_DIR, f"{table_name}{FD_SUFFIX}")


def get_fds(table_name):
    """Load stored functional dependencies for a table.

    Returns:
        list[dict] or None: FD list if available, otherwise None.
    """
    path = _fd_file_path(table_name)
    if not os.path.exists(path):
        return None

    with open(path, "r") as f:
        data = json.load(f)

    fds = data.get("fds", [])
    return fds if fds else None


def save_fds(table_name, fds):
    """Persist functional dependencies for a table."""
    os.makedirs(FD_META_DIR, exist_ok=True)
    path = _fd_file_path(table_name)

    payload = {
        "table": table_name,
        "fds": fds,
    }

    with open(path, "w") as f:
        json.dump(payload, f, indent=2)
