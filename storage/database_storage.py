"""
Storage operations for DATABASE management
"""
import os
import json
from visualizer import print_trace, print_result

DATA_DIR = "data"
META_DIR = "metadata"
DATABASES_FILE = os.path.join(META_DIR, "databases.json")


def load_databases():
    """Load list of all databases"""
    if not os.path.exists(DATABASES_FILE):
        return []
    
    try:
        with open(DATABASES_FILE, "r") as f:
            return json.load(f)
    except:
        return []


def save_databases(databases):
    """Save list of databases"""
    os.makedirs(META_DIR, exist_ok=True)
    with open(DATABASES_FILE, "w") as f:
        json.dump(databases, f, indent=2)


def create_database(db_name):
    """
    Create a new database
    """
    databases = load_databases()
    
    if db_name in databases:
        raise Exception(f"Database '{db_name}' already exists")
    
    # Create database directories
    db_data_dir = os.path.join(DATA_DIR, db_name)
    db_meta_dir = os.path.join(META_DIR, db_name)
    
    os.makedirs(db_data_dir, exist_ok=True)
    os.makedirs(db_meta_dir, exist_ok=True)
    
    # Add database to list
    databases.append(db_name)
    save_databases(databases)
    
    print_trace("DATABASE ENGINE", [
        f"Created database directory: {db_data_dir}",
        f"Created metadata directory: {db_meta_dir}"
    ])
    
    print_trace("FILE SYSTEM", [
        f"Database '{db_name}' created successfully"
    ])


def drop_database(db_name):
    """
    Drop an existing database
    """
    databases = load_databases()
    
    if db_name not in databases:
        raise Exception(f"Database '{db_name}' does not exist")
    
    # Remove database directories
    db_data_dir = os.path.join(DATA_DIR, db_name)
    db_meta_dir = os.path.join(META_DIR, db_name)
    db_index_dir = os.path.join("index", db_name)
    
    # Remove all files in directories
    import shutil
    
    if os.path.exists(db_data_dir):
        shutil.rmtree(db_data_dir)
    
    if os.path.exists(db_meta_dir):
        shutil.rmtree(db_meta_dir)

    if os.path.exists(db_index_dir):
        shutil.rmtree(db_index_dir)
    
    # Remove from database list
    databases.remove(db_name)
    save_databases(databases)
    
    print_trace("DATABASE ENGINE", [
        f"Dropped database: {db_name}",
        f"Removed index artifacts: {db_index_dir}"
    ])


def show_databases():
    """
    Show all databases
    """
    databases = load_databases()
    return databases


def database_exists(db_name):
    """
    Check if database exists
    """
    databases = load_databases()
    return db_name in databases
