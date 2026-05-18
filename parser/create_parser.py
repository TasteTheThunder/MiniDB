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


def _is_primary_key_segment(segment):
    if not segment:
        return False

    if segment[0] == "PRIMARY":
        return len(segment) > 1 and segment[1] == "KEY"

    if segment[0] == "CONSTRAINT":
        return len(segment) > 3 and segment[2] == "PRIMARY" and segment[3] == "KEY"

    return False


def _is_foreign_key_segment(segment):
    if not segment:
        return False

    if segment[0] == "FOREIGN" and len(segment) > 1 and segment[1] == "KEY":
        return True

    if segment[0] == "CONSTRAINT" and "FOREIGN" in segment and "KEY" in segment:
        return True

    return False


def _parse_foreign_key_segment(segment):
    if "FOREIGN" not in segment or "KEY" not in segment:
        raise Exception("Invalid FOREIGN KEY syntax")

    fk_index = segment.index("FOREIGN")
    if fk_index + 1 >= len(segment) or segment[fk_index + 1] != "KEY":
        raise Exception("Invalid FOREIGN KEY syntax")

    if "(" not in segment or ")" not in segment:
        raise Exception("Invalid FOREIGN KEY syntax")

    fk_paren_start = segment.index("(", fk_index + 1)
    fk_paren_end = segment.index(")", fk_paren_start)
    fk_columns = [
        segment[i]
        for i in range(fk_paren_start + 1, fk_paren_end)
        if segment[i] != ","
    ]

    if len(fk_columns) != 1:
        raise Exception("FOREIGN KEY must include exactly one column")

    if "REFERENCES" not in segment:
        raise Exception("FOREIGN KEY must include REFERENCES")

    ref_index = segment.index("REFERENCES", fk_paren_end + 1)
    if ref_index + 1 >= len(segment):
        raise Exception("Invalid REFERENCES syntax")

    ref_table = segment[ref_index + 1]

    if "(" not in segment[ref_index + 1:]:
        raise Exception("Invalid REFERENCES syntax")

    ref_paren_start = segment.index("(", ref_index + 1)
    ref_paren_end = segment.index(")", ref_paren_start)

    ref_columns = [
        segment[i]
        for i in range(ref_paren_start + 1, ref_paren_end)
        if segment[i] != ","
    ]

    if len(ref_columns) != 1:
        raise Exception("REFERENCES must include exactly one column")

    return {
        "column": fk_columns[0],
        "references_table": ref_table,
        "references_column": ref_columns[0]
    }


def parse_create(tokens):
    """
    Parse CREATE TABLE statement
    Syntax: CREATE TABLE table_name (col1 type1, col2 type2, ...)
            Supports inline PRIMARY KEY and table-level PRIMARY KEY.
    """
    if len(tokens) < 4:
        raise Exception("Invalid CREATE TABLE syntax. Use: CREATE TABLE name (...);")

    if tokens[0] != "CREATE" or tokens[1] != "TABLE":
        raise Exception("Invalid CREATE TABLE syntax. Use: CREATE TABLE name (...);")

    if tokens[3] != "(":
        raise Exception("Invalid CREATE TABLE syntax. Use: CREATE TABLE name (...);")

    table = tokens[2]

    start = tokens.index("(")
    end = _find_matching_paren(tokens, start)
    if end is None:
        raise Exception("Invalid CREATE TABLE syntax - missing closing parenthesis")

    if end != len(tokens) - 1:
        raise Exception("Invalid CREATE TABLE syntax. Use: CREATE TABLE name (...);")

    columns = []
    primary_key = None
    table_level_pk = None
    foreign_keys = []

    column_tokens = tokens[start + 1:end]
    segments = _split_column_segments(column_tokens)

    for segment in segments:
        if not segment:
            continue

        if _is_primary_key_segment(segment):
            if primary_key is not None or table_level_pk is not None:
                raise Exception("Multiple PRIMARY KEY declarations are not allowed")
            table_level_pk = _parse_primary_key_segment(segment)
            continue

        if _is_foreign_key_segment(segment):
            foreign_keys.append(_parse_foreign_key_segment(segment))
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
        if "PRIMARY" in segment and "KEY" in segment:
            if primary_key is not None or table_level_pk is not None:
                raise Exception("Multiple PRIMARY KEY declarations are not allowed")
            primary_key = col_name

        columns.append((col_name, col_type))

    if table_level_pk is not None:
        primary_key = table_level_pk

    # Disallow table-level PRIMARY KEY after the closing ')'.
    if end + 1 < len(tokens) and tokens[end + 1] == "PRIMARY":
        raise Exception(
            "PRIMARY KEY must be declared inside the CREATE TABLE parentheses"
        )

    command = {
        "type": "CREATE",
        "table": table,
        "columns": columns,
        "primary_key": primary_key,
        "foreign_keys": foreign_keys
    }

    return command
