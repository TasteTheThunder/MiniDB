"""
Parser for CREATE TABLE command
"""


def _find_matching_paren(tokens, start_index):
    depth = 0
    for i in range(start_index, len(tokens)):
        if tokens[i] == "(":
            depth += 1
        elif tokens[i] == ")":
            depth -= 1
            if depth == 0:
                return i
    return None


def _split_column_segments(tokens):
    segments = []
    current = []
    depth = 0

    for token in tokens:
        if token == "(":
            depth += 1
            current.append(token)
            continue
        if token == ")":
            depth = max(depth - 1, 0)
            current.append(token)
            continue
        if token == "," and depth == 0:
            if current:
                segments.append(current)
                current = []
            continue
        current.append(token)

    if current:
        segments.append(current)

    return segments


def _parse_primary_key_segment(segment):
    if "(" not in segment or ")" not in segment:
        raise Exception("Invalid PRIMARY KEY syntax")

    start = segment.index("(")
    end = segment.index(")", start)
    pk_columns = [
        segment[i]
        for i in range(start + 1, end)
        if segment[i] != ","
    ]

    if not pk_columns:
        raise Exception("PRIMARY KEY must include at least one column")

    return pk_columns if len(pk_columns) > 1 else pk_columns[0]


def parse_create(tokens):
    """
    Parse CREATE TABLE statement
    Syntax: CREATE TABLE table_name (col1 type1, col2 type2, ...)
            Supports inline PRIMARY KEY and table-level PRIMARY KEY.
    """
    table = tokens[2]

    start = tokens.index("(")
    end = _find_matching_paren(tokens, start)
    if end is None:
        raise Exception("Invalid CREATE TABLE syntax - missing closing parenthesis")

    columns = []
    primary_key = None
    table_level_pk = None

    column_tokens = tokens[start + 1:end]
    segments = _split_column_segments(column_tokens)

    for segment in segments:
        if not segment:
            continue

        if segment[0] == "PRIMARY":
            table_level_pk = _parse_primary_key_segment(segment)
            continue

        col_name = segment[0]
        if len(segment) < 2:
            raise Exception(f"Invalid column definition for '{col_name}'")

        col_type = segment[1]

        # Skip size specifier: VARCHAR ( 50 ), CHAR ( 1 ), etc.
        idx = 2
        if idx < len(segment) and segment[idx] == "(":
            depth = 1
            idx += 1
            while idx < len(segment) and depth > 0:
                if segment[idx] == "(":
                    depth += 1
                elif segment[idx] == ")":
                    depth -= 1
                idx += 1

        # Inline PRIMARY KEY (e.g., id INT PRIMARY KEY)
        if "PRIMARY" in segment and "KEY" in segment and primary_key is None:
            primary_key = col_name

        columns.append((col_name, col_type))

    if table_level_pk is not None:
        primary_key = table_level_pk

    # Support trailing table-level PRIMARY KEY after the column list.
    if end + 1 < len(tokens) and tokens[end + 1] == "PRIMARY":
        if table_level_pk is not None or primary_key is not None:
            raise Exception("Multiple PRIMARY KEY declarations are not allowed")
        trailing_segment = tokens[end + 1:]
        primary_key = _parse_primary_key_segment(trailing_segment)

    command = {
        "type": "CREATE",
        "table": table,
        "columns": columns,
        "primary_key": primary_key,
    }

    return command
