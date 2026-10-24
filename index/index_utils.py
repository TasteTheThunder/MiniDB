"""
Utility functions for index operations
"""
import os
import re

INDEX_DIR = "index"
OFFSETS_EXT = "offsets"
_ROW_OFFSET_CACHE = {}


def _index_base_dir(database=None):
    if database:
        return os.path.join(INDEX_DIR, database)
    return INDEX_DIR


def get_index_path(table, column, index_type, database=None):
    """
    Get the file path for an index.
    
    Args:
        table: Table name
        column: Column name
        index_type: 'hash' or 'sorted'
    
    Returns:
        File path for the index
    """
    filename = f"{table}_{column}.{index_type}"
    return os.path.join(_index_base_dir(database), filename)


def get_offsets_path(table, database=None):
    """Get the file path for row offsets."""
    filename = f"{table}.{OFFSETS_EXT}"
    return os.path.join(_index_base_dir(database), filename)


def _offset_cache_key(table, database=None):
    return (database or "__default__", table)


def _file_signature(path):
    try:
        stat = os.stat(path)
        return (stat.st_mtime_ns, stat.st_size)
    except OSError:
        return None


def _cache_row_offsets(tbl, table, offsets, database=None):
    path = get_offsets_path(table, database)
    _ROW_OFFSET_CACHE[_offset_cache_key(table, database)] = {
        "offsets": offsets,
        "table_signature": _file_signature(tbl),
        "offset_signature": _file_signature(path),
    }


def _write_row_offsets(path, offsets):
    with open(path, "w") as f:
        for offset in offsets:
            f.write(f"{offset}\n")


def _offsets_match_table(tbl, offsets):
    offset_index = 0

    with open(tbl, "rb") as f:
        while True:
            position = f.tell()
            line = f.readline()
            if not line:
                break
            if offset_index >= len(offsets) or offsets[offset_index] != position:
                return False
            offset_index += 1

    return offset_index == len(offsets)


def invalidate_row_offsets(table, database=None):
    """Discard cached and persisted offsets for a changed table."""
    _ROW_OFFSET_CACHE.pop(_offset_cache_key(table, database), None)

    path = get_offsets_path(table, database)
    if os.path.exists(path):
        os.remove(path)
        return True
    return False


def invalidate_database_row_offsets(database):
    """Discard all cached offsets for a database that is being removed."""
    cache_key_prefix = database or "__default__"
    for cache_key in list(_ROW_OFFSET_CACHE):
        if cache_key[0] == cache_key_prefix:
            del _ROW_OFFSET_CACHE[cache_key]


def build_row_offsets(tbl, table, database=None):
    """
    Build and persist row byte offsets for a table file.

    Returns:
        List of byte offsets for each row (0-indexed).
    """
    ensure_index_dir(database)
    offsets = []

    with open(tbl, "rb") as f:
        while True:
            pos = f.tell()
            line = f.readline()
            if not line:
                break
            offsets.append(pos)

    path = get_offsets_path(table, database)
    _write_row_offsets(path, offsets)
    _cache_row_offsets(tbl, table, offsets, database)

    return offsets


def build_row_offsets_and_load_rows(tbl, table, database=None):
    """Build row offsets while loading table rows for index construction."""
    ensure_index_dir(database)
    offsets = []
    rows = []

    with open(tbl, "rb") as f:
        while True:
            position = f.tell()
            line = f.readline()
            if not line:
                break
            offsets.append(position)
            rows.append(line.decode("utf-8").strip().split(","))

    path = get_offsets_path(table, database)
    _write_row_offsets(path, offsets)
    _cache_row_offsets(tbl, table, offsets, database)

    return rows


def load_row_offsets(tbl, table, database=None):
    """
    Load row offsets, rebuilding if missing or stale.

    Returns:
        List of byte offsets for each row (0-indexed).
    """
    path = get_offsets_path(table, database)
    cache_key = _offset_cache_key(table, database)
    cached = _ROW_OFFSET_CACHE.get(cache_key)
    if cached:
        if (
            cached["table_signature"] == _file_signature(tbl)
            and cached["offset_signature"] == _file_signature(path)
        ):
            return cached["offsets"]
        _ROW_OFFSET_CACHE.pop(cache_key, None)

    if not os.path.exists(path):
        return build_row_offsets(tbl, table, database)

    offset_signature = _file_signature(path)
    table_signature = _file_signature(tbl)
    if (
        offset_signature is None
        or table_signature is None
        or offset_signature[0] < table_signature[0]
    ):
        return build_row_offsets(tbl, table, database)

    offsets = []
    try:
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                offsets.append(int(line))
    except (OSError, ValueError):
        return build_row_offsets(tbl, table, database)

    if not _offsets_match_table(tbl, offsets):
        return build_row_offsets(tbl, table, database)

    _cache_row_offsets(tbl, table, offsets, database)
    return offsets


def ensure_index_dir(database=None):
    """Create index directory if it doesn't exist"""
    base_dir = _index_base_dir(database)
    if not os.path.exists(base_dir):
        os.makedirs(base_dir, exist_ok=True)


def index_exists(table, column, index_type, database=None):
    """Check if an index file exists"""
    path = get_index_path(table, column, index_type, database)
    return os.path.exists(path)


def load_index_data(table, column, index_type, database=None):
    """
    Load index data from file.
    
    Args:
        table: Table name
        column: Column name
        index_type: 'hash' or 'sorted'
    
    Returns:
        Index data (dict for hash, list for sorted) or None if not found
    """
    path = get_index_path(table, column, index_type, database)
    
    if not os.path.exists(path):
        return None
    
    try:
        with open(path, 'r') as f:
            if index_type == 'hash':
                return load_hash_index(f)
            elif index_type == 'sorted':
                return load_sorted_index(f)
    except Exception as e:
        print(f"Error loading index {path}: {e}")
        return None
    
    return None


def load_hash_index(file_obj):
    """
    Load hash index from file.
    
    Supported formats:
    1) Legacy: value:row_number
    2) Readable:
       {
         value -> row0
         value2 -> row1
       }
    """
    index_data = {}
    pattern = re.compile(r"^(.*?)\s*(?::|->)\s*(?:row)?(\d+)\s*,?$")

    for line in file_obj:
        line = line.strip()
        if not line or line in ("{", "}"):
            continue
        
        try:
            match = pattern.match(line)
            if not match:
                continue

            value = match.group(1).strip()
            row_num = int(match.group(2))
            
            if value not in index_data:
                index_data[value] = []
            index_data[value].append(row_num)
        except ValueError:
            continue
    
    return index_data if index_data else None


def load_sorted_index(file_obj):
    """
    Load sorted index from file.
    
    Supported formats:
    1) Legacy: value,row_number
    2) Readable:
       [
         (value, row0),
         (value2, row1)
       ]

    Returns list of (value, row_number) tuples sorted by value
    """
    index_data = []
    tuple_pattern = re.compile(r"^\(?\s*(.*?)\s*,\s*(?:row)?(\d+)\s*\)?\s*,?$")

    for line in file_obj:
        line = line.strip()
        if not line or line in ("[", "]"):
            continue
        
        try:
            match = tuple_pattern.match(line)
            if not match:
                continue

            value = match.group(1).strip()
            row_num = int(match.group(2))
            index_data.append((value, row_num))
        except (ValueError, IndexError):
            continue
    
    return index_data if index_data else None


def save_hash_index(table, column, index_data, database=None):
    """
    Save hash index to file.
    
    Args:
        table: Table name
        column: Column name
        index_data: Dict mapping values to list of row numbers
    """
    ensure_index_dir(database)
    path = get_index_path(table, column, 'hash', database)
    
    with open(path, 'w') as f:
        f.write("{\n")
        for value, row_numbers in sorted(index_data.items()):
            for row_num in sorted(row_numbers):
                f.write(f"  {value} -> row{row_num}\n")
        f.write("}\n")


def save_sorted_index(table, column, index_data, database=None):
    """
    Save sorted index to file.
    
    Args:
        table: Table name
        column: Column name
        index_data: List of (value, row_number) tuples
    """
    ensure_index_dir(database)
    path = get_index_path(table, column, 'sorted', database)
    
    with open(path, 'w') as f:
        f.write("[\n")
        for i, (value, row_num) in enumerate(index_data):
            suffix = "," if i < len(index_data) - 1 else ""
            f.write(f"  ({value}, row{row_num}){suffix}\n")
        f.write("]\n")


def delete_index(table, column, index_type, database=None):
    """Delete an index file"""
    path = get_index_path(table, column, index_type, database)
    if os.path.exists(path):
        try:
            os.remove(path)
            return True
        except Exception as e:
            print(f"Error deleting index {path}: {e}")
            return False
    return False


def list_indices(table, database=None):
    """
    List all indices for a table.
    
    Returns:
        List of (column, index_type) tuples
    """
    indices = []
    
    base_dir = _index_base_dir(database)

    if not os.path.exists(base_dir):
        return indices

    for filename in os.listdir(base_dir):
        if filename.startswith(table + "_"):
            parts = filename.replace(table + "_", "").rsplit(".", 1)
            if len(parts) == 2:
                column = parts[0]
                index_type = parts[1]
                if index_type in ['hash', 'sorted']:
                    indices.append((column, index_type))
    
    return indices
