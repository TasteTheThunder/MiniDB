"""
Parser for ALTER TABLE command
"""

def parse_alter(tokens):
    """
    Parse ALTER TABLE statement
    Supports: ADD COLUMN, DROP COLUMN, MODIFY COLUMN, RENAME COLUMN, RENAME TABLE,
              ADD PRIMARY KEY, DROP PRIMARY KEY
    """
    if tokens[1] != "TABLE":
        raise Exception("Invalid ALTER syntax")

    table = tokens[2]

    # Determine ALTER operation type
    operation = tokens[3].upper()

    # ============ ALTER TABLE ... RENAME TO new_table_name ============
    if operation == "RENAME":
        if tokens[4] == "TO":
            # ALTER TABLE old_name RENAME TO new_name
            new_table_name = tokens[5]
            command = {
                "type": "ALTER",
                "table": table,
                "operation": "RENAME_TABLE",
                "new_table_name": new_table_name
            }
        elif tokens[4] == "COLUMN":
            # ALTER TABLE table RENAME COLUMN old_col TO new_col
            if tokens[6] != "TO":
                raise Exception("Invalid RENAME COLUMN syntax")
            old_column = tokens[5]
            new_column = tokens[7]
            command = {
                "type": "ALTER",
                "table": table,
                "operation": "RENAME_COLUMN",
                "old_column": old_column,
                "new_column": new_column
            }
        else:
            raise Exception("Invalid RENAME syntax")

    # ============ ALTER TABLE ... ADD ============
    elif operation == "ADD":
        # Check what we're adding
        if tokens[4] == "COLUMN":
            # ADD COLUMN - supports multiple columns
            # ALTER TABLE students ADD COLUMN age INT, grade DOUBLE
            columns_to_add = []
            i = 5
            while i < len(tokens):
                if i + 1 >= len(tokens):
                    break
                col_name = tokens[i]
                col_type = tokens[i + 1]
                columns_to_add.append((col_name, col_type))
                i += 2
                # Skip comma if present
                if i < len(tokens) and tokens[i] == ",":
                    i += 1
                else:
                    break
            
            command = {
                "type": "ALTER",
                "table": table,
                "operation": "ADD_COLUMN",
                "columns": columns_to_add  # List of (name, type) tuples
            }
        
        elif tokens[4] == "PRIMARY" or (tokens[4] == "CONSTRAINT" and tokens[5] == "PRIMARY"):
            # ADD PRIMARY KEY or ADD CONSTRAINT PRIMARY KEY
            # ALTER TABLE students ADD PRIMARY KEY (id)
            # ALTER TABLE students ADD PRIMARY KEY (id, name)
            # ALTER TABLE students ADD CONSTRAINT PRIMARY KEY (id)
            
            if tokens[4] == "CONSTRAINT":
                # Skip CONSTRAINT keyword
                start_idx = 7  # after CONSTRAINT PRIMARY KEY (
            else:
                start_idx = 6  # after PRIMARY KEY (
            
            # Find columns between ( and )
            if "(" not in tokens:
                raise Exception("Invalid ADD PRIMARY KEY syntax - missing parentheses")
            
            paren_start = tokens.index("(", 4)
            paren_end = tokens.index(")", paren_start)
            
            # Extract column names
            pk_columns = [
                tokens[i]
                for i in range(paren_start + 1, paren_end)
                if tokens[i] != ","
            ]
            
            command = {
                "type": "ALTER",
                "table": table,
                "operation": "ADD_PRIMARY_KEY",
                "primary_key_columns": pk_columns
            }
        
        else:
            raise Exception("Invalid ADD syntax - use ADD COLUMN or ADD PRIMARY KEY")

    # ============ ALTER TABLE ... DROP ============
    elif operation == "DROP":
        # Check what we're dropping
        if tokens[4] == "COLUMN":
            # DROP COLUMN
            column_name = tokens[5]
            command = {
                "type": "ALTER",
                "table": table,
                "operation": "DROP_COLUMN",
                "column_name": column_name
            }
        
        elif tokens[4] == "PRIMARY" or (tokens[4] == "CONSTRAINT" and tokens[5] == "PRIMARY"):
            # DROP PRIMARY KEY or DROP CONSTRAINT PRIMARY KEY
            # ALTER TABLE students DROP PRIMARY KEY
            # ALTER TABLE students DROP CONSTRAINT PRIMARY KEY
            command = {
                "type": "ALTER",
                "table": table,
                "operation": "DROP_PRIMARY_KEY"
            }
        
        else:
            raise Exception("Invalid DROP syntax - use DROP COLUMN or DROP PRIMARY KEY")

    # ============ ALTER TABLE ... MODIFY COLUMN col_name new_datatype ============
    elif operation == "MODIFY":
        if tokens[4] != "COLUMN":
            raise Exception("Invalid MODIFY syntax")
        column_name = tokens[5]
        new_datatype = tokens[6]
        command = {
            "type": "ALTER",
            "table": table,
            "operation": "MODIFY_COLUMN",
            "column_name": column_name,
            "new_datatype": new_datatype
        }

    else:
        raise Exception(f"Unsupported ALTER operation: {operation}")
    
    return command
