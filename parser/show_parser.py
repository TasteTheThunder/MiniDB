"""
Parser for SHOW TABLES command
"""

def parse_show(tokens):
    """
    Parse SHOW TABLES statement
    Syntax: SHOW TABLES
    """
    if len(tokens) != 2 or tokens[0] != "SHOW" or tokens[1] != "TABLES":
        raise Exception("Invalid SHOW syntax. Use: SHOW TABLES;")

    command = {
        "type": "SHOW_TABLES"
    }
    
    return command
