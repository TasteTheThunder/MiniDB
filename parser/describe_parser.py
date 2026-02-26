"""
Parser for DESCRIBE TABLE command
"""

def parse_describe(tokens):
    """
    Parse DESCRIBE statement
    Syntax: DESCRIBE table_name
    """
    if len(tokens) < 2:
        raise Exception("Invalid DESCRIBE syntax - table name required")

    table = tokens[1]

    command = {
        "type": "DESCRIBE",
        "table": table
    }
    
    return command
