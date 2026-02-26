"""
Storage operations for TRUNCATE TABLE
"""
from visualizer import print_trace, print_result
from utils import table_paths, check_table_exists


def truncate_table(command):
    """Delete all rows from a table while keeping the structure"""
    
    table = command["table"]
    tbl, meta = table_paths(table)
    check_table_exists(tbl, meta)
    
    # Count existing rows before truncation
    rows = open(tbl).readlines()
    row_count = len(rows)
    
    # Clear the table data
    open(tbl, "w").write("")
    
    print_trace("STORAGE ENGINE", [
        f"Truncating table: {table}",
        f"Deleted {row_count} row(s)",
        "Table structure preserved"
    ])
    
    print_result(f"✅ Table {table} Truncated - {row_count} row(s) deleted")
