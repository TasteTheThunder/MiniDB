"""
Parser for UPDATE command
"""

def parse_update(tokens):
    """
    Parse UPDATE statement
    Syntax: UPDATE table SET column = value WHERE condition
    """
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
    
    return command
