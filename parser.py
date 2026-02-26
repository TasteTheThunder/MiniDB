from tokenizer import tokenize
from visualizer import print_trace
import config


def parse_query(query):

    tokens = tokenize(query)

    if not tokens:

        raise Exception("Empty query")

    command_type = tokens[0].upper()


    # =====================================================
    # CREATE TABLE
    # =====================================================

    if command_type == "CREATE":

        table = tokens[2]

        start = tokens.index("(") + 1

        columns = []

        primary_key = None

        i = start

        while i < len(tokens):

            token = tokens[i]

            # stop when PRIMARY KEY starts

            if token == "PRIMARY":

                # PRIMARY KEY ( id ) or PRIMARY KEY ( id, name )
                # Find the opening and closing parentheses
                pk_start = i + 2  # After "PRIMARY KEY"
                if tokens[pk_start] == "(":
                    pk_end = tokens.index(")", pk_start)
                    # Extract all column names between parentheses
                    pk_columns = [
                        tokens[j]
                        for j in range(pk_start + 1, pk_end)
                        if tokens[j] != ","
                    ]
                    # Store as list if multiple columns, single string if one
                    primary_key = pk_columns if len(pk_columns) > 1 else pk_columns[0]
                else:
                    # Fallback for old syntax without parentheses
                    primary_key = tokens[i + 2]

                break

            if token == ")":

                break

            col_name = tokens[i]

            col_type = tokens[i + 1]

            columns.append(

                (col_name, col_type)

            )

            i += 3   # skip comma

        command = {

            "type": "CREATE",

            "table": table,

            "columns": columns,

            "primary_key": primary_key

        }


    # =====================================================
    # INSERT
    # =====================================================

    elif command_type == "INSERT":

        table = tokens[2]

        if "VALUES" not in tokens:

            raise Exception(

                "Invalid INSERT syntax"

            )

        values_index = tokens.index("VALUES")

        # -----------------------------------
        # COLUMN LIST (OPTIONAL)
        # -----------------------------------

        column_list = None

        # INSERT INTO students (id,name)

        if tokens[3] == "(":

            col_start = 3

            col_end = tokens.index(")",col_start)

            column_list = [

                tokens[i]

                for i in range(col_start+1,col_end)

                if tokens[i] != ","

            ]

        # -----------------------------------
        # VALUES (...), (...), ...
        # -----------------------------------

        # Find all value sets (multiple rows support)
        values_list = []
        i = values_index + 1
        
        while i < len(tokens):
            if tokens[i] == "(":
                # Find matching closing paren
                start = i + 1
                depth = 1
                j = start
                
                while depth > 0 and j < len(tokens):
                    if tokens[j] == "(":
                        depth += 1
                    elif tokens[j] == ")":
                        depth -= 1
                    j += 1
                
                end = j - 1
                
                # Extract values between ( and )
                values = [
                    tokens[k]
                    for k in range(start, end)
                    if tokens[k] != ","
                ]
                values_list.append(values)
                i = j
            else:
                i += 1

        command={

            "type":"INSERT",

            "table":table,

            "values":values_list,  # Now a list of value lists

            "columns":column_list

        }

    # =====================================================
    # SELECT
    # =====================================================

    elif command_type == "SELECT":

        from_index = tokens.index("FROM")

        select_columns = [
            t for t in tokens[1:from_index]
            if t != ","
        ]

        table = tokens[from_index + 1]

        condition = None
        group_by = None
        order_by = None
        limit = None
        aggregate = None
        agg_column = None


        # ---------------- WHERE ----------------
        if "WHERE" in tokens:
            idx = tokens.index("WHERE")
            condition = (
                tokens[idx + 1],   # column
                tokens[idx + 2],   # operator
                tokens[idx + 3]    # value
            )


        # ---------------- AGGREGATE ----------------
        aggregates = ["COUNT", "SUM", "AVG", "MIN", "MAX"]

        if select_columns:

            # Check all columns for aggregate functions
            for col in select_columns:
                if col.upper() in aggregates:
                    aggregate = col.upper()
                    # Find the column being aggregated
                    if "(" in tokens:
                        # Find position of this aggregate in tokens
                        for i, t in enumerate(tokens):
                            if t.upper() == aggregate and i + 1 < len(tokens) and tokens[i + 1] == "(":
                                agg_column = tokens[i + 2]
                                break
                    break


        # ---------------- GROUP BY ----------------
        if "GROUP" in tokens and "BY" in tokens:
            g = tokens.index("GROUP")
            group_by = tokens[g + 2]


        # ---------------- ORDER BY ----------------
        if "ORDER" in tokens and "BY" in tokens:
            o = tokens.index("ORDER")
            column = tokens[o + 2]

            order = "ASC"
            if len(tokens) > o + 3:
                if tokens[o + 3] in ["ASC", "DESC"]:
                    order = tokens[o + 3]

            order_by = (column, order)


        # ---------------- LIMIT ----------------
        if "LIMIT" in tokens:
            l = tokens.index("LIMIT")
            limit = int(tokens[l + 1])


        # ---------------- FINAL COMMAND ----------------
        # Filter out aggregate function tokens from columns
        non_agg_columns = select_columns
        if aggregate:
            # Remove aggregate-related tokens: aggregate name, '(', column, ')'
            non_agg_columns = [
                c for c in select_columns 
                if c.upper() not in ["COUNT", "SUM", "AVG", "MIN", "MAX", "(", ")"]
                and c != agg_column
            ]
            # If we have GROUP BY, use non-aggregate columns, otherwise None
            if group_by:
                select_columns = non_agg_columns if non_agg_columns else None
            else:
                select_columns = None
        
        command = {
            "type": "SELECT",
            "table": table,
            "columns": select_columns,
            "aggregate": aggregate,
            "agg_column": agg_column,
            "condition": condition,
            "group_by": group_by,
            "order_by": order_by,
            "limit": limit
        }

    # =====================================================
    # DELETE
    # =====================================================

    elif command_type == "DELETE":

        # DELETE FROM table WHERE condition
        if "FROM" not in tokens or "WHERE" not in tokens:
            raise Exception("Invalid DELETE syntax")

        from_index = tokens.index("FROM")
        table = tokens[from_index + 1]

        where_index = tokens.index("WHERE")
        
        condition = (
            tokens[where_index + 1],   # column
            tokens[where_index + 2],   # operator
            tokens[where_index + 3]    # value
        )

        command = {
            "type": "DELETE",
            "table": table,
            "condition": condition
        }

    # =====================================================
    # DROP TABLE
    # =====================================================

    elif command_type == "DROP":

        table = tokens[2]

        command = {

            "type": "DROP",

            "table": table

        }


    # =====================================================
    # UPDATE
    # =====================================================

    elif command_type == "UPDATE":

        table = tokens[1]

        if "SET" not in tokens or "WHERE" not in tokens:

            raise Exception("Invalid UPDATE syntax")

        set_index = tokens.index("SET")

        where_index = tokens.index("WHERE")

        set_column = tokens[set_index + 1]

        set_value = tokens[set_index + 3]

        condition = (

            tokens[where_index + 1],   # column
            tokens[where_index + 2],   # operator
            tokens[where_index + 3]    # value

        )

        command = {

            "type":"UPDATE",

            "table":table,

            "set":(

                set_column,
                set_value

            ),

            "condition":condition

        }

    # =====================================================
    # ALTER TABLE
    # =====================================================

    elif command_type == "ALTER":

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

    # =====================================================
    # SHOW TABLES
    # =====================================================

    elif command_type == "SHOW":

        if len(tokens) < 2 or tokens[1] != "TABLES":
            raise Exception("Invalid SHOW syntax - use SHOW TABLES")

        command = {
            "type": "SHOW_TABLES"
        }

    # =====================================================
    # DESCRIBE TABLE
    # =====================================================

    elif command_type == "DESCRIBE":

        if len(tokens) < 2:
            raise Exception("Invalid DESCRIBE syntax - table name required")

        table = tokens[1]

        command = {
            "type": "DESCRIBE",
            "table": table
        }

    # =====================================================
    # TRUNCATE TABLE
    # =====================================================

    elif command_type == "TRUNCATE":

        # TRUNCATE TABLE students or TRUNCATE students
        if tokens[1] == "TABLE":
            table = tokens[2]
        else:
            table = tokens[1]

        command = {
            "type": "TRUNCATE",
            "table": table
        }

    else:
        raise Exception(f"Unsupported command: {command_type}")

# =====================================================
# EDUCATIONAL VISUALIZATION
# =====================================================

    if config.get_mode() == "EDUCATIONAL":

        trace_lines = [

            f"Operation Type : {command['type']}"

        ]
        
        # Add table name if present (not for SHOW TABLES)
        if command.get('table'):
            trace_lines.append(f"Target Table   : {command['table']}")


        # ================= CREATE =================

        if command["type"] == "CREATE":

            trace_lines.append(

                f"Columns        : {command['columns']}"

            )

            pk = command.get('primary_key')
            if pk:
                if isinstance(pk, list):
                    trace_lines.append(
                        f"Primary Key    : Composite ({', '.join(pk)})"
                    )
                else:
                    trace_lines.append(
                        f"Primary Key    : {pk}"
                    )
            else:
                trace_lines.append(
                    f"Primary Key    : None"
                )


        # ================= INSERT =================

        if command["type"] == "INSERT":

            values = command['values']
            # Check if multiple rows
            if values and isinstance(values[0], list):
                trace_lines.append(
                    f"Rows to Insert : {len(values)}"
                )
                trace_lines.append(
                    f"First Row      : {values[0]}"
                )
            else:
                trace_lines.append(
                    f"Values         : {values}"
                )


        # ================= SELECT =================

        if command["type"] == "SELECT":

            if command.get("aggregate"):

                trace_lines.append(

                    f"Aggregate      : {command['aggregate']}({command.get('agg_column')})"

                )
                
                if command.get("group_by"):
                    trace_lines.append(
                        f"Group By       : {command['group_by']}"
                    )

            else:

                trace_lines.append(

                    f"Target Columns : {command['columns']}"

                )


        # ================= UPDATE =================

        if command["type"] == "UPDATE":

            trace_lines.append(

                f"Set            : {command['set'][0]} = {command['set'][1]}"

            )


        # ================= ALTER =================

        if command["type"] == "ALTER":

            operation = command["operation"]
            trace_lines.append(f"Operation      : {operation}")

            if operation == "ADD_COLUMN":
                columns = command['columns']
                if len(columns) == 1:
                    trace_lines.append(f"Column Name    : {columns[0][0]}")
                    trace_lines.append(f"Column Type    : {columns[0][1]}")
                else:
                    trace_lines.append(f"Columns to Add : {len(columns)}")
                    for col_name, col_type in columns:
                        trace_lines.append(f"  - {col_name} ({col_type})")

            elif operation == "DROP_COLUMN":
                trace_lines.append(f"Column Name    : {command['column_name']}")

            elif operation == "MODIFY_COLUMN":
                trace_lines.append(f"Column Name    : {command['column_name']}")
                trace_lines.append(f"New Datatype   : {command['new_datatype']}")

            elif operation == "RENAME_COLUMN":
                trace_lines.append(f"Old Column     : {command['old_column']}")
                trace_lines.append(f"New Column     : {command['new_column']}")

            elif operation == "RENAME_TABLE":
                trace_lines.append(f"New Table Name : {command['new_table_name']}")
            
            elif operation == "ADD_PRIMARY_KEY":
                pk_cols = command['primary_key_columns']
                if len(pk_cols) == 1:
                    trace_lines.append(f"Primary Key    : {pk_cols[0]}")
                else:
                    trace_lines.append(f"Primary Key    : Composite ({', '.join(pk_cols)})")
            
            elif operation == "DROP_PRIMARY_KEY":
                trace_lines.append(f"Action         : Remove Primary Key Constraint")


        # ================= SHOW TABLES =================

        if command["type"] == "SHOW_TABLES":
            trace_lines.append("Action         : List all tables in database")


        # ================= DESCRIBE =================

        if command["type"] == "DESCRIBE":
            trace_lines.append("Action         : Show table structure")


        # ================= TRUNCATE =================

        if command["type"] == "TRUNCATE":
            trace_lines.append("Action         : Delete all rows from table")


        # ================= CONDITION =================

        if command.get("condition"):

            cond = command["condition"]

            # Support (= > <)

            if len(cond) == 3:

                trace_lines.append(

                    f"Condition      : {cond[0]} {cond[1]} {cond[2]}"

                )

            else:

                trace_lines.append(

                    f"Condition      : {cond[0]} = {cond[1]}"

                )


        print_trace(

            "PARSER",
            trace_lines

        )

    return command