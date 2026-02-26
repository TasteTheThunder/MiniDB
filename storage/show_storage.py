"""
Storage operations for SHOW TABLES
"""
import os
from visualizer import print_trace, print_result

DATA_DIR = "data"


def show_tables():
    """Display all tables in the database"""
    
    if not os.path.exists(DATA_DIR):
        print_trace("STORAGE ENGINE", [
            "No data directory found",
            "No tables exist"
        ])
        print_result("No tables in database")
        return
    
    tables = [f.replace(".tbl", "") for f in os.listdir(DATA_DIR) if f.endswith(".tbl")]
    
    if not tables:
        print_trace("STORAGE ENGINE", [
            "Data directory exists but is empty",
            "No tables found"
        ])
        print_result("No tables in database")
        return
    
    tables.sort()
    
    print_trace("STORAGE ENGINE", [
        f"Found {len(tables)} table(s)",
        f"Tables: {', '.join(tables)}"
    ])
    
    # Display results in a table format
    print("\n" + "=" * 40)
    print(f"{'TABLE NAME':^40}")
    print("=" * 40)
    
    for table in tables:
        print(f"{table:^40}")
    
    print("=" * 40)
    print(f"\n{len(tables)} table(s) found\n")
