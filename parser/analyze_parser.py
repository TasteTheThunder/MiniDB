"""Parser for ANALYZE SCHEMA command."""


def parse_analyze(tokens):
    """Parse ANALYZE SCHEMA statement.

    Syntax: ANALYZE SCHEMA table_name
    """
    if len(tokens) != 3 or tokens[0] != "ANALYZE" or tokens[1] != "SCHEMA":
        raise Exception("Invalid ANALYZE syntax. Use: ANALYZE SCHEMA table_name;")

    return {
        "type": "ANALYZE_SCHEMA",
        "table": tokens[2],
    }
