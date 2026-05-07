"""
Parser for DELETE command
"""

def parse_delete(tokens):
    """
    Parse DELETE statement
    Syntax: DELETE FROM table WHERE condition
    """
    # Expected tokens: DELETE FROM <table> WHERE <column> <op> <value>
    if len(tokens) != 7:
        raise Exception("Invalid DELETE syntax. Use: DELETE FROM table WHERE condition;")

    if tokens[0] != "DELETE" or tokens[1] != "FROM" or tokens[3] != "WHERE":
        raise Exception("Invalid DELETE syntax.")

    table = tokens[2]
    column = tokens[4]
    operator = tokens[5]
    value = tokens[6]

    if operator not in ["=", ">", "<", ">=", "<=", "!="]:
        raise Exception("Invalid DELETE syntax.")

    condition = (column, operator, value)

    command = {
        "type": "DELETE",
        "table": table,
        "condition": condition
    }
    
    return command
