"""
Parser for CREATE TABLE command
"""

def parse_create(tokens):
    """
    Parse CREATE TABLE statement
    Syntax: CREATE TABLE table_name (col1 type1, col2 type2, ...) PRIMARY KEY (col)
    """
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
    
    return command
