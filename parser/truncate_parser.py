"""
Parser for TRUNCATE TABLE command
"""

def parse_truncate(tokens):
    """
    Parse TRUNCATE statement
    Syntax: TRUNCATE TABLE table_name OR TRUNCATE table_name
    """
    if tokens[0] != "TRUNCATE":
        raise Exception("Invalid TRUNCATE syntax. Use: TRUNCATE TABLE table_name;")

    if len(tokens) == 3 and tokens[1] == "TABLE":
        table = tokens[2]
    elif len(tokens) == 2:
        table = tokens[1]
    else:
        raise Exception("Invalid TRUNCATE syntax. Use: TRUNCATE TABLE table_name;")

    command = {
        "type": "TRUNCATE",
        "table": table
    }
    
    return command
