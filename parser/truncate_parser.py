"""
Parser for TRUNCATE TABLE command
"""

def parse_truncate(tokens):
    """
    Parse TRUNCATE statement
    Syntax: TRUNCATE TABLE table_name OR TRUNCATE table_name
    """
    # TRUNCATE TABLE students or TRUNCATE students
    if tokens[1] == "TABLE":
        table = tokens[2]
    else:
        table = tokens[1]

    command = {
        "type": "TRUNCATE",
        "table": table
    }
    
    return command
