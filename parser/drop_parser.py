"""
Parser for DROP TABLE command
"""

def parse_drop(tokens):
    """
    Parse DROP TABLE statement
    Syntax: DROP TABLE table_name
    """
    table = tokens[2]

    command = {

        "type": "DROP",

        "table": table

    }
    
    return command
