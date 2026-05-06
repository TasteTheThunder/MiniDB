"""
Utility functions for index operations
"""
import os
import re

INDEX_DIR = "index"


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
