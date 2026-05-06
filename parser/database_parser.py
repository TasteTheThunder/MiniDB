"""
Parser for DATABASE commands
"""
from tokenizer import tokenize


def parse_database(tokens):
    """
    Parse database-level commands:
    - CREATE DATABASE db_name
    - DROP DATABASE db_name
    - SHOW DATABASES
    - USE db_name
    """
    
    if not tokens or len(tokens) < 2:
        raise Exception("Invalid database command")
    
    command_type = tokens[0].upper()
    
    if command_type == "CREATE":
        return parse_create_database(tokens)
    elif command_type == "DROP":
        return parse_drop_database(tokens)
    elif command_type == "SHOW":
        return parse_show_databases(tokens)
    elif command_type == "USE":
        return parse_use_database(tokens)
    else:
        raise Exception(f"Unknown database command: {command_type}")


def parse_create_database(tokens):
    """
    Parse: CREATE DATABASE db_name;
    """
    if len(tokens) < 3:
        raise Exception("Invalid CREATE DATABASE syntax")
    
    if tokens[1].upper() != "DATABASE":
        raise Exception("Expected 'DATABASE' after CREATE")
    
    db_name = tokens[2].replace(";", "").strip()
    
    if not db_name:
        raise Exception("Database name cannot be empty")
    
    return {
        "type": "CREATE_DATABASE",
        "database": db_name
    }


def parse_drop_database(tokens):
    """
    Parse: DROP DATABASE db_name;
    """
    if len(tokens) < 3:
        raise Exception("Invalid DROP DATABASE syntax")
    
    if tokens[1].upper() != "DATABASE":
        raise Exception("Expected 'DATABASE' after DROP")
    
    db_name = tokens[2].replace(";", "").strip()
    
    if not db_name:
        raise Exception("Database name cannot be empty")
    
    return {
        "type": "DROP_DATABASE",
        "database": db_name
    }


def parse_show_databases(tokens):
    """
    Parse: SHOW DATABASES;
    """
    if len(tokens) < 2:
        raise Exception("Invalid SHOW DATABASES syntax")
    
    if tokens[1].upper() != "DATABASES":
        raise Exception("Expected 'DATABASES' after SHOW")
    
    return {
        "type": "SHOW_DATABASES"
    }


def parse_use_database(tokens):
    """
    Parse: USE db_name;
    """
    if len(tokens) < 2:
        raise Exception("Invalid USE syntax")
    
    db_name = tokens[1].replace(";", "").strip()
    
    if not db_name:
        raise Exception("Database name cannot be empty")
    
    return {
        "type": "USE_DATABASE",
        "database": db_name
    }
