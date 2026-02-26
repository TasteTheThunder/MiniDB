"""
Parser for DELETE command
"""

def parse_delete(tokens):
    """
    Parse DELETE statement
    Syntax: DELETE FROM table WHERE condition
    """
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
    
    return command
