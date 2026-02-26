import os
import json
import config
from visualizer import print_trace, print_result
from utils import (
    table_paths,
    check_table_exists,
    remove_quotes,
    compare,
    validate_value
)


DATA_DIR="data"
META_DIR="metadata"

SUPPORTED_TYPES=["INT","DOUBLE","CHAR","VARCHAR"]

# ================= CREATE =================

def create_table(table,columns,primary_key=None):

    tbl,meta=table_paths(table)

    if os.path.exists(tbl):

        raise Exception("Table already exists")

    # datatype validation

    for name,dtype in columns:

        if dtype.upper() not in SUPPORTED_TYPES:

            raise Exception(

                f"Unsupported datatype {dtype}"

            )

    # Normalize primary_key to always be a list (for composite key support)
    if primary_key:
        if isinstance(primary_key, str):
            primary_key = [primary_key]  # Convert single key to list
        
        names = [c[0] for c in columns]
        
        # Validate all primary key columns exist
        for pk_col in primary_key:
            if pk_col not in names:
                raise Exception(
                    f"Primary Key column '{pk_col}' must be valid column"
                )

    open(tbl,"w").close()

    with open(meta,"w") as f:

        json.dump({

            "columns":columns,
            "primary_key":primary_key

        },f)

    print_trace("STORAGE ENGINE",[

        f"Created Data File : {tbl}",
        f"Created Metadata : {meta}"

    ])

    print_trace("FILE SYSTEM",[

        f"{table}.tbl initialized"

    ])

    print_result("✅ Table Created Successfully")


# ================= INSERT =================

# ================= INSERT =================

def insert_row(table,values,insert_columns=None):

    tbl,meta = table_paths(table)

    check_table_exists(tbl,meta)

    metadata=json.load(open(meta))

    column_defs=metadata["columns"]

    columns=[c[0] for c in column_defs]

    pk=metadata.get("primary_key")


    # =====================================
    # BUILD FINAL ROW (NULL DEFAULT)
    # =====================================

    final_values=["NULL"]*len(columns)


    # ---------- OLD INSERT ----------

    if insert_columns is None:

        if len(values)!=len(columns):

            raise Exception(

                "Column count mismatch"

            )

        insert_columns=columns


    # ---------- COLUMN INSERT ----------

    if len(insert_columns)!=len(values):

        raise Exception(

            "Column count mismatch"

        )


    # fill values

    for col,value in zip(insert_columns,values):

        if col not in columns:

            raise Exception(

                f"Column {col} not found"

            )

        idx=columns.index(col)

        datatype=column_defs[idx][1]

        validate_value(

            value,
            datatype

        )

        if datatype.upper() in ["CHAR","VARCHAR"]:

            value=remove_quotes(value)

        final_values[idx]=value


    # =====================================
    # PRIMARY KEY CHECK
    # =====================================

    if pk:
        # Support both single and composite primary keys
        if isinstance(pk, str):
            pk = [pk]  # Convert to list for uniform handling
        
        # Get indices for all primary key columns
        pk_indices = [columns.index(pk_col) for pk_col in pk]
        
        # Check that none of the PK columns are NULL
        for i, pk_col in zip(pk_indices, pk):
            if final_values[i] == "NULL":
                raise Exception(
                    f"Primary Key column '{pk_col}' cannot be NULL"
                )
        
        # Duplicate check - compare composite key
        with open(tbl) as f:
            for row in f:
                existing = row.strip().split(",")
                
                # Check if all PK columns match (composite key duplicate check)
                if all(existing[i] == final_values[i] for i in pk_indices):
                    if len(pk) == 1:
                        raise Exception(
                            f"Primary Key violation: {pk[0]} = {final_values[pk_indices[0]]}"
                        )
                    else:
                        pk_values = ", ".join([f"{pk[i]}={final_values[pk_indices[i]]}" for i in range(len(pk))])
                        raise Exception(
                            f"Composite Primary Key violation: ({pk_values})"
                        )


    # =====================================
    # WRITE FILE
    # =====================================

    line=",".join(final_values)

    open(tbl,"a").write(line+"\n")


    print_trace("STORAGE ENGINE",[

        f"Open File : {tbl}",
        "Mode : Append",
        f"Data Written : {line}"

    ])


    print_trace("FILE SYSTEM",[

        f"{table}.tbl updated"

    ])


    print_result("✅ Row Inserted Successfully")


# ================= SELECT =================

def select_rows(
        table,
        condition,
        selected_columns=None,
        aggregate=None,
        agg_column=None,
        group_by=None,
        order_by=None,
        limit=None
):

    tbl,meta=table_paths(table)

    check_table_exists(tbl,meta)

    metadata=json.load(open(meta))

    columns=[c[0] for c in metadata["columns"]]

    rows=open(tbl).readlines()

    print_trace("STORAGE ENGINE",[

        f"Open File : {tbl}",
        f"Rows Scanned : {len(rows)}"

    ])

    filtered=[]

    for row in rows:

        vals=row.strip().split(",")

        if condition:

            col,op,val=condition

            idx=columns.index(col)

            if not compare(vals[idx],op,val):

                continue

        filtered.append(vals)

    # ===========================
    # ORDER BY
    # ===========================

    if order_by:
        sort_col, sort_order = order_by
        sort_idx = columns.index(sort_col)
        
        # Try to sort numerically, fall back to string sort
        try:
            filtered.sort(
                key=lambda row: float(row[sort_idx]) if row[sort_idx] != "NULL" else float('-inf'),
                reverse=(sort_order == "DESC")
            )
        except (ValueError, IndexError):
            filtered.sort(
                key=lambda row: row[sort_idx],
                reverse=(sort_order == "DESC")
            )

    # ===========================
    # LIMIT
    # ===========================

    if limit:
        filtered = filtered[:limit]

    # ===========================
    # AGGREGATE PART
    # ===========================

    if aggregate:

        # ===========================
        # GROUP BY
        # ===========================

        if group_by:
            grp_idx = columns.index(group_by)
            groups = {}
            
            for row in filtered:
                grp_val = row[grp_idx]
                if grp_val not in groups:
                    groups[grp_val] = []
                groups[grp_val].append(row)
            
            # Determine aggregate column name for display
            agg_display = f"{aggregate}({agg_column if agg_column else '*'})"
            
            print(f"\n{group_by} | {agg_display}")
            print("-" * 40)
            
            for grp_val, grp_rows in groups.items():
                if aggregate == "COUNT":
                    result = len(grp_rows)
                else:
                    idx = columns.index(agg_column)
                    nums = [float(r[idx]) for r in grp_rows if r[idx] != "NULL"]
                    
                    if not nums:
                        result = "NULL"
                    elif aggregate == "SUM":
                        result = sum(nums)
                    elif aggregate == "AVG":
                        result = sum(nums) / len(nums)
                    elif aggregate == "MIN":
                        result = min(nums)
                    elif aggregate == "MAX":
                        result = max(nums)
                
                print(f"{grp_val} | {result}")
            
            print_trace("FILE SYSTEM", [
                "Grouped aggregate computed"
            ])
            
            print_result("✅ Aggregate Operation Completed")
            return

        # ===========================
        # SIMPLE AGGREGATE (NO GROUP BY)
        # ===========================

        if aggregate=="COUNT":

            result=len(filtered)

            print(f"\nCOUNT = {result}")

        else:

            idx=columns.index(agg_column)

            nums=[ float(r[idx]) for r in filtered ]

            if not nums:

                print("No rows")

                return

            if aggregate=="SUM":

                result=sum(nums)

            elif aggregate=="AVG":

                result=sum(nums)/len(nums)

            elif aggregate=="MIN":

                result=min(nums)

            elif aggregate=="MAX":

                result=max(nums)

            print(f"\n{aggregate}({agg_column}) = {result}")

        print_trace("FILE SYSTEM",[

            "Aggregate computed"

        ])

        print_result("✅ Aggregate Operation Completed")

        return

    # ======================
    # NORMAL SELECT
    # ======================

    if selected_columns==["*"]:

        selected_columns=columns

    indexes=[columns.index(c) for c in selected_columns]

    print("\nResult:")

    print(" | ".join(selected_columns))

    for r in filtered:

        print(" | ".join([r[i] for i in indexes]))

    print_trace("FILE SYSTEM",[

        f"{len(filtered)} row(s) returned"

    ])

    print_result("✅ SELECT Operation Completed")

# ================= DELETE =================

def delete_row(table,condition):

    tbl,meta=table_paths(table)

    check_table_exists(tbl,meta)

    metadata=json.load(open(meta))

    columns=[c[0] for c in metadata["columns"]]

    cond_col,op,val=condition

    ci=columns.index(cond_col)

    new=[]
    deleted=0

    for row in open(tbl):

        vals=row.strip().split(",")

        if compare(

            vals[ci],
            op,
            val

        ):

            deleted+=1

        else:

            new.append(row)

    open(tbl,"w").writelines(new)

    print_trace("STORAGE ENGINE",[

        f"Deleting rows where {cond_col} {op} {val}",
        f"Rows Deleted : {deleted}"

    ])

    print_trace("FILE SYSTEM",[

        f"{table}.tbl rewritten"

    ])

    print_result("✅ DELETE Completed")


# ================= DROP =================

def drop_table(table):

    tbl,meta=table_paths(table)

    check_table_exists(tbl,meta)

    os.remove(tbl)
    os.remove(meta)

    print_trace("STORAGE ENGINE",[

        f"Deleted {tbl}",
        f"Deleted {meta}"

    ])

    print_trace("FILE SYSTEM",[

        f"{table} removed"

    ])

    print_result("✅ Table Dropped")


# ================= UPDATE =================

def update_row(table,set_data,condition):

    tbl,meta=table_paths(table)

    check_table_exists(tbl,meta)

    metadata=json.load(open(meta))

    columns=[c[0] for c in metadata["columns"]]

    set_col,set_val=set_data

    cond_col,op,cond_val=condition

    dtype=None

    for c in metadata["columns"]:

        if c[0]==set_col:

            dtype=c[1]

    validate_value(set_val,dtype)

    if dtype.upper() in ["CHAR","VARCHAR"]:

        set_val=remove_quotes(set_val)

    si=columns.index(set_col)
    ci=columns.index(cond_col)

    new=[]
    updated=0

    for row in open(tbl):

        vals=row.strip().split(",")

        if compare(

            vals[ci],
            op,
            cond_val

        ):

            vals[si]=set_val

            updated+=1

        new.append(",".join(vals)+"\n")

    open(tbl,"w").writelines(new)

    print_trace("STORAGE ENGINE",[

        f"Updating rows where {cond_col} {op} {cond_val}",
        f"Rows Updated : {updated}"

    ])

    print_trace("FILE SYSTEM",[

        f"{table}.tbl rewritten"

    ])

    print_result("✅ UPDATE Completed")


# ================= ALTER TABLE =================

def alter_table(command):
    """
    Handles all ALTER TABLE operations:
    - ADD_COLUMN: Add a new column
    - DROP_COLUMN: Remove a column
    - MODIFY_COLUMN: Change column datatype
    - RENAME_COLUMN: Rename a column
    - RENAME_TABLE: Rename the table
    """
    
    table = command["table"]
    operation = command["operation"]
    tbl, meta = table_paths(table)
    check_table_exists(tbl, meta)
    
    metadata = json.load(open(meta))
    columns = metadata["columns"]
    primary_key = metadata.get("primary_key")
    
    # =========================
    # ADD COLUMN
    # =========================
    if operation == "ADD_COLUMN":
        columns_to_add = command["columns"]  # List of (name, type) tuples
        
        column_names = [c[0] for c in columns]
        
        # Validate all columns
        for column_name, column_type in columns_to_add:
            # Validate datatype
            if column_type.upper() not in SUPPORTED_TYPES:
                raise Exception(f"Unsupported datatype {column_type}")
            
            # Check if column already exists
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
                # Add NULL for each new column
                nulls = ",NULL" * len(columns_to_add)
                new_rows.append(row + nulls + "\n")
        
        # Write updated data
        open(tbl, "w").writelines(new_rows)
        
        # Save metadata
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
    
    # =========================
    # DROP COLUMN
    # =========================
    elif operation == "DROP_COLUMN":
        column_name = command["column_name"]
        
        # Check if column exists
        column_names = [c[0] for c in columns]
        if column_name not in column_names:
            raise Exception(f"Column {column_name} does not exist")
        
        # Cannot drop primary key column(s)
        if primary_key:
            pk_list = primary_key if isinstance(primary_key, list) else [primary_key]
            if column_name in pk_list:
                raise Exception(f"Cannot drop primary key column '{column_name}'")
        
        # Find column index
        col_index = column_names.index(column_name)
        
        # Remove column from metadata
        columns.pop(col_index)
        metadata["columns"] = columns
        
        # Update all rows to remove column data
        rows = open(tbl).readlines()
        new_rows = []
        for row in rows:
            vals = row.strip().split(",")
            vals.pop(col_index)
            new_rows.append(",".join(vals) + "\n")
        
        # Write updated data
        open(tbl, "w").writelines(new_rows)
        
        # Save metadata
        with open(meta, "w") as f:
            json.dump(metadata, f)
        
        print_trace("STORAGE ENGINE", [
            f"Dropped column: {column_name}",
            f"Updated {len(new_rows)} row(s)"
        ])
        
        print_result(f"✅ Column {column_name} Dropped Successfully")
    
    # =========================
    # MODIFY COLUMN
    # =========================
    elif operation == "MODIFY_COLUMN":
        column_name = command["column_name"]
        new_datatype = command["new_datatype"]
        
        # Validate new datatype
        if new_datatype.upper() not in SUPPORTED_TYPES:
            raise Exception(f"Unsupported datatype {new_datatype}")
        
        # Check if column exists
        column_names = [c[0] for c in columns]
        if column_name not in column_names:
            raise Exception(f"Column {column_name} does not exist")
        
        # Find column index and update datatype
        col_index = column_names.index(column_name)
        old_datatype = columns[col_index][1]
        columns[col_index] = (column_name, new_datatype)
        metadata["columns"] = columns
        
        # Note: We don't convert existing data, just update the schema
        # In a real DB, you might want to validate/convert existing data
        
        # Save metadata
        with open(meta, "w") as f:
            json.dump(metadata, f)
        
        print_trace("STORAGE ENGINE", [
            f"Modified column: {column_name}",
            f"Datatype changed: {old_datatype} → {new_datatype}"
        ])
        
        print_result(f"✅ Column {column_name} Modified Successfully")
    
    # =========================
    # RENAME COLUMN
    # =========================
    elif operation == "RENAME_COLUMN":
        old_column = command["old_column"]
        new_column = command["new_column"]
        
        # Check if old column exists
        column_names = [c[0] for c in columns]
        if old_column not in column_names:
            raise Exception(f"Column {old_column} does not exist")
        
        # Check if new column name already exists
        if new_column in column_names:
            raise Exception(f"Column {new_column} already exists")
        
        # Find column index and rename
        col_index = column_names.index(old_column)
        datatype = columns[col_index][1]
        columns[col_index] = (new_column, datatype)
        metadata["columns"] = columns
        
        # Update primary key if needed (handle both single and composite keys)
        if primary_key:
            if isinstance(primary_key, str):
                if primary_key == old_column:
                    metadata["primary_key"] = new_column
            elif isinstance(primary_key, list):
                # Update composite primary key
                metadata["primary_key"] = [
                    new_column if pk_col == old_column else pk_col 
                    for pk_col in primary_key
                ]
        
        # Save metadata
        with open(meta, "w") as f:
            json.dump(metadata, f)
        
        print_trace("STORAGE ENGINE", [
            f"Renamed column: {old_column} → {new_column}"
        ])
        
        print_result(f"✅ Column Renamed Successfully")
    
    # =========================
    # RENAME TABLE
    # =========================
    elif operation == "RENAME_TABLE":
        new_table_name = command["new_table_name"]
        
        # Get new paths
        new_tbl, new_meta = table_paths(new_table_name)
        
        # Check if new table name already exists
        if os.path.exists(new_tbl) or os.path.exists(new_meta):
            raise Exception(f"Table {new_table_name} already exists")
        
        # Rename files
        os.rename(tbl, new_tbl)
        os.rename(meta, new_meta)
        
        print_trace("STORAGE ENGINE", [
            f"Renamed table: {table} → {new_table_name}",
            f"Data file: {tbl} → {new_tbl}",
            f"Metadata file: {meta} → {new_meta}"
        ])
        
        print_result(f"✅ Table Renamed to {new_table_name}")
    
    # =========================
    # ADD PRIMARY KEY
    # =========================
    elif operation == "ADD_PRIMARY_KEY":
        pk_columns = command["primary_key_columns"]
        
        # Check if primary key already exists
        if primary_key:
            pk_display = primary_key if isinstance(primary_key, str) else ", ".join(primary_key)
            raise Exception(f"Table already has a primary key: {pk_display}")
        
        # Validate all PK columns exist
        column_names = [c[0] for c in columns]
        for pk_col in pk_columns:
            if pk_col not in column_names:
                raise Exception(f"Column '{pk_col}' does not exist")
        
        # Check for NULL values in existing data
        rows = open(tbl).readlines()
        pk_indices = [column_names.index(pk_col) for pk_col in pk_columns]
        
        for row_num, row in enumerate(rows, 1):
            vals = row.strip().split(",")
            for i, pk_col in zip(pk_indices, pk_columns):
                if vals[i] == "NULL":
                    raise Exception(
                        f"Cannot add PRIMARY KEY: Column '{pk_col}' has NULL values (row {row_num})"
                    )
        
        # Check for duplicate composite keys in existing data
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
        
        # Set primary key (always store as list for consistency)
        metadata["primary_key"] = pk_columns
        
        # Save metadata
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
    
    # =========================
    # DROP PRIMARY KEY
    # =========================
    elif operation == "DROP_PRIMARY_KEY":
        # Check if primary key exists
        if not primary_key:
            raise Exception("Table does not have a primary key")
        
        # Get current primary key for display
        if isinstance(primary_key, str):
            pk_display = primary_key
        else:
            pk_display = ", ".join(primary_key)
        
        # Remove primary key
        metadata["primary_key"] = None
        
        # Save metadata
        with open(meta, "w") as f:
            json.dump(metadata, f)
        
        print_trace("STORAGE ENGINE", [
            f"Dropped PRIMARY KEY constraint: {pk_display}"
        ])
        
        print_result(f"✅ PRIMARY KEY Dropped: {pk_display}")
    
    else:
        raise Exception(f"Unsupported ALTER operation: {operation}")


# ================= SHOW TABLES =================

def show_tables():
    """
    Display all tables in the database by listing metadata files
    """
    
    # Get all .meta files in metadata directory
    if not os.path.exists(META_DIR):
        print("\nNo tables found.")
        print_result("✅ SHOW TABLES Completed")
        return
    
    meta_files = [f for f in os.listdir(META_DIR) if f.endswith(".meta")]
    
    if not meta_files:
        print("\nNo tables found.")
        print_result("✅ SHOW TABLES Completed")
        return
    
    # Extract table names (remove .meta extension)
    table_names = [f[:-5] for f in meta_files]
    table_names.sort()
    
    print_trace("STORAGE ENGINE", [
        f"Scanning metadata directory: {META_DIR}",
        f"Found {len(table_names)} table(s)"
    ])
    
    print("\nTables in database:")
    print("─" * 40)
    for i, table in enumerate(table_names, 1):
        print(f"{i}. {table}")
    print("─" * 40)
    
    print_result(f"✅ {len(table_names)} table(s) found")


# ================= DESCRIBE TABLE =================

def describe_table(table):
    """
    Show the structure of a table (columns, datatypes, primary key)
    """
    
    tbl, meta = table_paths(table)
    check_table_exists(tbl, meta)
    
    metadata = json.load(open(meta))
    columns = metadata["columns"]
    primary_key = metadata.get("primary_key")
    
    print_trace("STORAGE ENGINE", [
        f"Reading metadata: {meta}",
        f"Columns: {len(columns)}"
    ])
    
    # Display table structure
    print(f"\nTable: {table}")
    print("=" * 60)
    print(f"{'Column Name':<20} {'Data Type':<15} {'Key'}")
    print("-" * 60)
    
    # Normalize primary key to list for easier checking
    pk_list = []
    if primary_key:
        if isinstance(primary_key, str):
            pk_list = [primary_key]
        elif isinstance(primary_key, list):
            pk_list = primary_key
    
    for col_name, col_type in columns:
        key_indicator = ""
        if col_name in pk_list:
            if len(pk_list) > 1:
                key_indicator = "PK (Composite)"
            else:
                key_indicator = "PRIMARY KEY"
        
        print(f"{col_name:<20} {col_type:<15} {key_indicator}")
    
    print("=" * 60)
    
    # Show primary key info
    if primary_key:
        if isinstance(primary_key, str):
            print(f"Primary Key: {primary_key}")
        elif isinstance(primary_key, list):
            print(f"Composite Primary Key: ({', '.join(primary_key)})")
    else:
        print("Primary Key: None")
    
    print_trace("FILE SYSTEM", [
        f"Table structure displayed"
    ])
    
    print_result("✅ DESCRIBE Completed")


# ================= TRUNCATE TABLE =================

def truncate_table(table):
    """
    Remove all data from a table but keep the structure (metadata intact)
    """
    
    tbl, meta = table_paths(table)
    check_table_exists(tbl, meta)
    
    # Count existing rows before truncating
    rows = open(tbl).readlines()
    row_count = len(rows)
    
    # Empty the data file
    open(tbl, "w").close()
    
    print_trace("STORAGE ENGINE", [
        f"Truncating table: {table}",
        f"Rows deleted: {row_count}",
        "Metadata preserved"
    ])
    
    print_trace("FILE SYSTEM", [
        f"{table}.tbl emptied",
        f"{table}.meta unchanged"
    ])
    
    print_result(f"✅ TRUNCATE Completed - {row_count} row(s) removed")

