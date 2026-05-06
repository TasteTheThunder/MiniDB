"""
Storage operations for TRUNCATE TABLE
"""
from visualizer import print_trace, print_result
from utils import table_paths, check_table_exists


def truncate_table(command, database=None):
    """Delete all rows from a table while keeping the structure.
    If database is specified, truncates table in that database.
    """
    
    table = command["table"]
    tbl, meta = table_paths(table, database)
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
