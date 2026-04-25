"""
Utility functions for index operations
"""
import os
import json

INDEX_DIR = "index"


def get_index_path(table, column, index_type):
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
    return os.path.join(INDEX_DIR, filename)


def ensure_index_dir():
    """Create index directory if it doesn't exist"""
    if not os.path.exists(INDEX_DIR):
        os.makedirs(INDEX_DIR)


def index_exists(table, column, index_type):
    """Check if an index file exists"""
    path = get_index_path(table, column, index_type)
    return os.path.exists(path)


def load_index_data(table, column, index_type):
    """
    Load index data from file.
    
    Args:
        table: Table name
        column: Column name
        index_type: 'hash' or 'sorted'
    
    Returns:
        Index data (dict for hash, list for sorted) or None if not found
    """
    path = get_index_path(table, column, index_type)
    
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
    
    Format: value:row_number (one per line)
    """
    index_data = {}
    for line in file_obj:
        line = line.strip()
        if not line:
            continue
        
        try:
            value, row_num = line.split(':', 1)
            row_num = int(row_num)
            
            if value not in index_data:
                index_data[value] = []
            index_data[value].append(row_num)
        except ValueError:
            continue
    
    return index_data if index_data else None


def load_sorted_index(file_obj):
    """
    Load sorted index from file.
    
    Format: value,row_number (one per line)
    Returns list of (value, row_number) tuples sorted by value
    """
    index_data = []
    for line in file_obj:
        line = line.strip()
        if not line:
            continue
        
        try:
            parts = line.rsplit(',', 1)
            if len(parts) == 2:
                value = parts[0]
                row_num = int(parts[1])
                index_data.append((value, row_num))
        except (ValueError, IndexError):
            continue
    
    return index_data if index_data else None


def save_hash_index(table, column, index_data):
    """
    Save hash index to file.
    
    Args:
        table: Table name
        column: Column name
        index_data: Dict mapping values to list of row numbers
    """
    ensure_index_dir()
    path = get_index_path(table, column, 'hash')
    
    with open(path, 'w') as f:
        for value, row_numbers in sorted(index_data.items()):
            for row_num in sorted(row_numbers):
                f.write(f"{value}:{row_num}\n")


def save_sorted_index(table, column, index_data):
    """
    Save sorted index to file.
    
    Args:
        table: Table name
        column: Column name
        index_data: List of (value, row_number) tuples
    """
    ensure_index_dir()
    path = get_index_path(table, column, 'sorted')
    
    with open(path, 'w') as f:
        for value, row_num in index_data:
            f.write(f"{value},{row_num}\n")


def delete_index(table, column, index_type):
    """Delete an index file"""
    path = get_index_path(table, column, index_type)
    if os.path.exists(path):
        try:
            os.remove(path)
            return True
        except Exception as e:
            print(f"Error deleting index {path}: {e}")
            return False
    return False


def list_indices(table):
    """
    List all indices for a table.
    
    Returns:
        List of (column, index_type) tuples
    """
    indices = []
    
    if not os.path.exists(INDEX_DIR):
        return indices
    
    for filename in os.listdir(INDEX_DIR):
        if filename.startswith(table + "_"):
            parts = filename.replace(table + "_", "").rsplit(".", 1)
            if len(parts) == 2:
                column = parts[0]
                index_type = parts[1]
                if index_type in ['hash', 'sorted']:
                    indices.append((column, index_type))
    
    return indices
