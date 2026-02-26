"""
Storage operations for ALTER TABLE
"""
import os
import json
from visualizer import print_trace, print_result
from utils import table_paths, check_table_exists

DATA_DIR = "data"
META_DIR = "metadata"
SUPPORTED_TYPES = ["INT", "DOUBLE", "CHAR", "VARCHAR"]


def alter_table(command):
    """
    Handles all ALTER TABLE operations:
    - ADD_COLUMN: Add a new column
    - DROP_COLUMN: Remove a column
    - MODIFY_COLUMN: Change column datatype
    - RENAME_COLUMN: Rename a column
    - RENAME_TABLE: Rename the table
    - ADD_PRIMARY_KEY: Add primary key constraint
    - DROP_PRIMARY_KEY: Remove primary key constraint
    """
    
    table = command["table"]
    operation = command["operation"]
    tbl, meta = table_paths(table)
    check_table_exists(tbl, meta)
    
    metadata = json.load(open(meta))
    columns = metadata["columns"]
    primary_key = metadata.get("primary_key")
    
    # ===================================
    # ADD COLUMN
    # ===================================
    if operation == "ADD_COLUMN":
        columns_to_add = command["columns"]  # List of (name, type) tuples
        
        column_names = [c[0] for c in columns]
        
        # Validate all columns
        for column_name, column_type in columns_to_add:
            if column_type.upper() not in SUPPORTED_TYPES:
                raise Exception(f"Unsupported datatype {column_type}")
            
            if column_name in column_names:
                raise Exception(f"Column {column_name} already exists")
        
        # Add all columns to metadata
        for col_name, col_type in columns_to_add:
            columns.append((col_name, col_type))
        metadata["columns"] = columns
        
        # Update all rows with NULL for each new column
        rows = open(tbl).readlines()
        new_rows = []
        for row in rows:
            row = row.strip()
            if row:
                nulls = ",NULL" * len(columns_to_add)
                new_rows.append(row + nulls + "\n")
        
        open(tbl, "w").writelines(new_rows)
        
        with open(meta, "w") as f:
            json.dump(metadata, f)
        
        if len(columns_to_add) == 1:
            col_name, col_type = columns_to_add[0]
            print_trace("STORAGE ENGINE", [
                f"Added column: {col_name} ({col_type})",
                f"Updated {len(new_rows)} row(s) with NULL values"
            ])
            print_result(f"✅ Column {col_name} Added Successfully")
        else:
            col_list = ", ".join([f"{name} ({dtype})" for name, dtype in columns_to_add])
            print_trace("STORAGE ENGINE", [
                f"Added {len(columns_to_add)} columns: {col_list}",
                f"Updated {len(new_rows)} row(s) with NULL values"
            ])
            print_result(f"✅ {len(columns_to_add)} Columns Added Successfully")
    
    # ===================================
    # DROP COLUMN
    # ===================================
    elif operation == "DROP_COLUMN":
        column_name = command["column_name"]
        
        column_names = [c[0] for c in columns]
        if column_name not in column_names:
            raise Exception(f"Column {column_name} does not exist")
        
        if primary_key:
            pk_list = primary_key if isinstance(primary_key, list) else [primary_key]
            if column_name in pk_list:
                raise Exception(f"Cannot drop primary key column '{column_name}'")
        
        col_index = column_names.index(column_name)
        columns.pop(col_index)
        metadata["columns"] = columns
        
        rows = open(tbl).readlines()
        new_rows = []
        for row in rows:
            vals = row.strip().split(",")
            vals.pop(col_index)
            new_rows.append(",".join(vals) + "\n")
        
        open(tbl, "w").writelines(new_rows)
        
        with open(meta, "w") as f:
            json.dump(metadata, f)
        
        print_trace("STORAGE ENGINE", [
            f"Dropped column: {column_name}",
            f"Updated {len(new_rows)} row(s)"
        ])
        
        print_result(f"✅ Column {column_name} Dropped Successfully")
    
    # ===================================
    # MODIFY COLUMN
    # ===================================
    elif operation == "MODIFY_COLUMN":
        column_name = command["column_name"]
        new_datatype = command["new_datatype"]
        
        if new_datatype.upper() not in SUPPORTED_TYPES:
            raise Exception(f"Unsupported datatype {new_datatype}")
        
        column_names = [c[0] for c in columns]
        if column_name not in column_names:
            raise Exception(f"Column {column_name} does not exist")
        
        col_index = column_names.index(column_name)
        old_datatype = columns[col_index][1]
        columns[col_index] = (column_name, new_datatype)
        metadata["columns"] = columns
        
        with open(meta, "w") as f:
            json.dump(metadata, f)
        
        print_trace("STORAGE ENGINE", [
            f"Modified column: {column_name}",
            f"Datatype changed: {old_datatype} → {new_datatype}"
        ])
        
        print_result(f"✅ Column {column_name} Modified Successfully")
    
    # ===================================
    # RENAME COLUMN
    # ===================================
    elif operation == "RENAME_COLUMN":
        old_column = command["old_column"]
        new_column = command["new_column"]
        
        column_names = [c[0] for c in columns]
        if old_column not in column_names:
            raise Exception(f"Column {old_column} does not exist")
        
        if new_column in column_names:
            raise Exception(f"Column {new_column} already exists")
        
        col_index = column_names.index(old_column)
        datatype = columns[col_index][1]
        columns[col_index] = (new_column, datatype)
        metadata["columns"] = columns
        
        if primary_key:
            if isinstance(primary_key, str):
                if primary_key == old_column:
                    metadata["primary_key"] = new_column
            elif isinstance(primary_key, list):
                metadata["primary_key"] = [
                    new_column if pk_col == old_column else pk_col 
                    for pk_col in primary_key
                ]
        
        with open(meta, "w") as f:
            json.dump(metadata, f)
        
        print_trace("STORAGE ENGINE", [
            f"Renamed column: {old_column} → {new_column}"
        ])
        
        print_result(f"✅ Column Renamed Successfully")
    
    # ===================================
    # RENAME TABLE
    # ===================================
    elif operation == "RENAME_TABLE":
        new_table_name = command["new_table_name"]
        
        new_tbl, new_meta = table_paths(new_table_name)
        
        if os.path.exists(new_tbl) or os.path.exists(new_meta):
            raise Exception(f"Table {new_table_name} already exists")
        
        os.rename(tbl, new_tbl)
        os.rename(meta, new_meta)
        
        print_trace("STORAGE ENGINE", [
            f"Renamed table: {table} → {new_table_name}",
            f"Data file: {tbl} → {new_tbl}",
            f"Metadata file: {meta} → {new_meta}"
        ])
        
        print_result(f"✅ Table Renamed to {new_table_name}")
    
    # ===================================
    # ADD PRIMARY KEY
    # ===================================
    elif operation == "ADD_PRIMARY_KEY":
        pk_columns = command["primary_key_columns"]
        
        if primary_key:
            pk_display = primary_key if isinstance(primary_key, str) else ", ".join(primary_key)
            raise Exception(f"Table already has a primary key: {pk_display}")
        
        column_names = [c[0] for c in columns]
        for pk_col in pk_columns:
            if pk_col not in column_names:
                raise Exception(f"Column '{pk_col}' does not exist")
        
        rows = open(tbl).readlines()
        pk_indices = [column_names.index(pk_col) for pk_col in pk_columns]
        
        for row_num, row in enumerate(rows, 1):
            vals = row.strip().split(",")
            for i, pk_col in zip(pk_indices, pk_columns):
                if vals[i] == "NULL":
                    raise Exception(
                        f"Cannot add PRIMARY KEY: Column '{pk_col}' has NULL values (row {row_num})"
                    )
        
        pk_values_set = set()
        for row_num, row in enumerate(rows, 1):
            vals = row.strip().split(",")
            pk_tuple = tuple(vals[i] for i in pk_indices)
            if pk_tuple in pk_values_set:
                pk_display = ", ".join([f"{pk_columns[i]}={vals[pk_indices[i]]}" for i in range(len(pk_columns))])
                raise Exception(
                    f"Cannot add PRIMARY KEY: Duplicate values found ({pk_display})"
                )
            pk_values_set.add(pk_tuple)
        
        metadata["primary_key"] = pk_columns
        
        with open(meta, "w") as f:
            json.dump(metadata, f)
        
        if len(pk_columns) == 1:
            print_trace("STORAGE ENGINE", [
                f"Added PRIMARY KEY constraint: {pk_columns[0]}"
            ])
            print_result(f"✅ PRIMARY KEY Added: {pk_columns[0]}")
        else:
            pk_list = ", ".join(pk_columns)
            print_trace("STORAGE ENGINE", [
                f"Added COMPOSITE PRIMARY KEY constraint: ({pk_list})"
            ])
            print_result(f"✅ Composite PRIMARY KEY Added: ({pk_list})")
    
    # ===================================
    # DROP PRIMARY KEY
    # ===================================
    elif operation == "DROP_PRIMARY_KEY":
        if not primary_key:
            raise Exception("Table does not have a primary key")
        
        if isinstance(primary_key, str):
            pk_display = primary_key
        else:
            pk_display = ", ".join(primary_key)
        
        metadata["primary_key"] = None
        
        with open(meta, "w") as f:
            json.dump(metadata, f)
        
        print_trace("STORAGE ENGINE", [
            f"Dropped PRIMARY KEY constraint: {pk_display}"
        ])
        
        print_result(f"✅ PRIMARY KEY Dropped: {pk_display}")
    
    else:
        raise Exception(f"Unsupported ALTER operation: {operation}")
