"""Shared helpers for schema analysis and normalization."""

import json
import re

from utils import table_paths, check_table_exists


def load_schema(table_name):
    """Load table schema from metadata."""
    tbl, meta = table_paths(table_name)
    check_table_exists(tbl, meta)

    with open(meta, "r") as f:
        metadata = json.load(f)

    columns = metadata.get("columns", [])
    primary_key = metadata.get("primary_key")

    attributes = [col[0] for col in columns]
    pk_list = normalize_primary_key(primary_key)

    return {
        "table": table_name,
        "columns": columns,
        "attributes": attributes,
        "primary_key": pk_list,
        "raw_primary_key": primary_key,
    }


def normalize_primary_key(primary_key):
    """Normalize primary key representation to list."""
    if not primary_key:
        return []
    if isinstance(primary_key, list):
        return primary_key
    return [primary_key]


def parse_fd_line(line):
    """Parse FD line in format: A -> B, C"""
    # Tolerate accidental prompt text in pasted input, e.g. "FD > sid -> name".
    line = re.sub(r"^(?:\s*FD\s*>\s*)+", "", line, flags=re.IGNORECASE).strip()

    if "->" not in line:
        raise ValueError("FD must contain '->'. Example: roll -> name, dept")

    left_text, right_text = line.split("->", 1)
    lhs = [attr.strip() for attr in left_text.split(",") if attr.strip()]
    rhs = [attr.strip() for attr in right_text.split(",") if attr.strip()]

    if not lhs or not rhs:
        raise ValueError("Both LHS and RHS are required. Example: dept -> hod")

    return {
        "lhs": lhs,
        "rhs": rhs,
    }


def parse_fd_input(lines):
    """Parse multiple FD input lines."""
    fds = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        fds.append(parse_fd_line(line))
    return fds


def format_fd(fd):
    """Convert FD dict to display string."""
    lhs = ", ".join(fd.get("lhs", []))
    rhs = ", ".join(fd.get("rhs", []))
    return f"{lhs} -> {rhs}"


def format_attribute_list(values):
    """Join attributes for output."""
    return ", ".join(values) if values else "None"
