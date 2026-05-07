"""
Parser for DESCRIBE TABLE command
"""

def parse_describe(tokens):
    """
    Parse DESCRIBE statement
    Syntax: DESCRIBE table_name
    """
    if len(tokens) != 2 or tokens[0] != "DESCRIBE":
        raise Exception("Invalid DESCRIBE syntax. Use: DESCRIBE table_name;")

    table = tokens[1]

    command = {
        "type": "DESCRIBE",
        "table": table
    }
    
    return command
