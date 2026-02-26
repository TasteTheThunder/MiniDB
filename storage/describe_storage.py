"""
Storage operations for DESCRIBE TABLE
"""
import json
from visualizer import print_trace, print_result
from utils import table_paths, check_table_exists


def describe_table(command):
    """Display table structure and metadata"""
    
    table = command["table"]
    tbl, meta = table_paths(table)
    check_table_exists(tbl, meta)
    
    metadata = json.load(open(meta))
    columns = metadata["columns"]
    primary_key = metadata.get("primary_key")
    
    # Count rows
    rows = open(tbl).readlines()
    row_count = len(rows)
    
    print_trace("STORAGE ENGINE", [
        f"Reading metadata for table: {table}",
        f"Columns: {len(columns)}",
        f"Rows: {row_count}"
    ])
    
    # Display table structure
    print("\n" + "=" * 80)
    print(f"TABLE: {table}")
    print("=" * 80)
    print(f"{'COLUMN NAME':<30} {'DATA TYPE':<20} {'KEY':<15}")
    print("-" * 80)
    
    for col_name, col_type in columns:
        key_info = ""
        
        if primary_key:
            if isinstance(primary_key, str):
                if col_name == primary_key:
                    key_info = "PRIMARY KEY"
            elif isinstance(primary_key, list):
                if col_name in primary_key:
                    key_info = "PRIMARY KEY"
        
        print(f"{col_name:<30} {col_type:<20} {key_info:<15}")
    
    print("=" * 80)
    
    # Display additional info
    if primary_key:
        if isinstance(primary_key, str):
            pk_display = primary_key
        else:
            pk_display = "(" + ", ".join(primary_key) + ")"
        print(f"Primary Key: {pk_display}")
    else:
        print("Primary Key: None")
    
    print(f"Total Rows: {row_count}")
    print("=" * 80 + "\n")
