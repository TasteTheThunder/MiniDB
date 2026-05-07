"""
Parser for UPDATE command
"""

def parse_update(tokens):
    """
    Parse UPDATE statement
    Syntax: UPDATE table SET column = value WHERE condition
    """
    # Expected tokens: UPDATE <table> SET <column> = <value> WHERE <column> <op> <value>
    if len(tokens) != 10:
        raise Exception("Invalid UPDATE syntax. Use: UPDATE table SET col = value WHERE condition;")

    if tokens[0] != "UPDATE" or tokens[2] != "SET" or tokens[4] != "=" or tokens[6] != "WHERE":
        raise Exception("Invalid UPDATE syntax. Use: UPDATE table SET col = value WHERE condition;")

    table = tokens[1]
    set_column = tokens[3]
    set_value = tokens[5]
    where_column = tokens[7]
    operator = tokens[8]
    where_value = tokens[9]

    if operator not in ["=", ">", "<", ">=", "<=", "!="]:
        raise Exception("Invalid UPDATE syntax. Use: UPDATE table SET col = value WHERE condition;")

    condition = (where_column, operator, where_value)

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
