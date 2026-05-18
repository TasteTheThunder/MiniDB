"""
Parser for SELECT command
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

def parse_select(tokens):
    """
    Parse SELECT statement
    Syntax: SELECT columns FROM table [WHERE condition] [GROUP BY col] [ORDER BY col] [LIMIT n]
    """
    if len(tokens) < 4 or tokens[0] != "SELECT":
        raise Exception("Invalid SELECT syntax. Use: SELECT ... FROM table;")

    if "FROM" not in tokens:
        raise Exception("Invalid SELECT syntax. Use: SELECT ... FROM table;")

    from_index = tokens.index("FROM")
    if from_index < 2 or from_index + 1 >= len(tokens):
        raise Exception("Invalid SELECT syntax. Use: SELECT ... FROM table;")

    select_columns = [
        t for t in tokens[1:from_index]
        if t != ","
    ]

    if not select_columns:
        raise Exception("Invalid SELECT syntax. Use: SELECT ... FROM table;")

    table = tokens[from_index + 1]

    condition = None
    join = None
    group_by = None
    having = None
    order_by = None
    limit = None
    aggregate = None
    agg_column = None

    aggregates = ["COUNT", "SUM", "AVG", "MIN", "MAX"]
    for col in select_columns:
        if col.upper() in aggregates:
            aggregate = col.upper()
            if "(" in tokens:
                for i, t in enumerate(tokens):
                    if t.upper() == aggregate and i + 2 < len(tokens) and tokens[i + 1] == "(":
                        agg_column = tokens[i + 2]
                        break
            break
        # If it looks like a function call but isn't a supported aggregate, fail early.
        if "(" in tokens:
            for i, t in enumerate(tokens):
                if t == col and i + 1 < len(tokens) and tokens[i + 1] == "(":
                    raise Exception("Invalid SELECT syntax. Use: SELECT ... FROM table;")

    i = from_index + 2
    stage = 0

    # ---------------- JOIN (optional) ----------------
    if i < len(tokens) and tokens[i] in ["INNER", "LEFT", "RIGHT", "JOIN"]:
        if tokens[i] == "JOIN":
            join_type = "INNER"
            i += 1
        else:
            join_type = tokens[i]
            i += 1
            if i >= len(tokens) or tokens[i] != "JOIN":
                raise Exception("Invalid SELECT syntax. Use: ... <JOIN> ... ON ...;")
            i += 1

        if i >= len(tokens):
            raise Exception("Invalid SELECT syntax. Use: ... <JOIN> ... ON ...;")

        join_table = tokens[i]
        i += 1

        if i >= len(tokens) or tokens[i] != "ON":
            raise Exception("Invalid SELECT syntax. Use: ... JOIN ... ON col = col;")

        if i + 3 >= len(tokens) or tokens[i + 2] != "=":
            raise Exception("Invalid SELECT syntax. JOIN ON requires '='.")

        left_column = tokens[i + 1]
        right_column = tokens[i + 3]
        i += 4

        join = {
            "type": join_type,
            "table": join_table,
            "left_column": left_column,
            "right_column": right_column
        }

    while i < len(tokens):
        token = tokens[i]

        if token == "WHERE":
            if stage > 0:
                raise Exception("Invalid SELECT syntax. Use: SELECT ... FROM table;")
            if i + 1 >= len(tokens):
                raise Exception("Invalid SELECT syntax. Use: SELECT ... FROM table;")

            # WHERE EXISTS (subquery)
            if tokens[i + 1] == "EXISTS":
                subquery, next_i = _parse_subquery(tokens, i + 2)
                condition = {
                    "type": "exists",
                    "subquery": subquery,
                    "negated": False
                }
                i = next_i
                stage = 1
                continue

            # WHERE NOT EXISTS (subquery)
            if tokens[i + 1] == "NOT":
                if i + 2 >= len(tokens) or tokens[i + 2] != "EXISTS":
                    raise Exception("Invalid SELECT syntax. Use: WHERE NOT EXISTS (...);")
                subquery, next_i = _parse_subquery(tokens, i + 3)
                condition = {
                    "type": "exists",
                    "subquery": subquery,
                    "negated": True
                }
                i = next_i
                stage = 1
                continue

            # WHERE <column> IN (subquery)
            if i + 3 < len(tokens) and tokens[i + 2] == "IN":
                column = tokens[i + 1]
                subquery, next_i = _parse_subquery(tokens, i + 3)
                condition = {
                    "type": "in",
                    "column": column,
                    "subquery": subquery
                }
                i = next_i
                stage = 1
                continue

            # WHERE <column> <op> <value>
            if i + 3 >= len(tokens):
                raise Exception("Invalid SELECT syntax. Use: SELECT ... FROM table;")
            if tokens[i + 2] not in ["=", ">", "<", ">=", "<=", "!="]:
                raise Exception("Invalid SELECT syntax. Use: SELECT ... FROM table;")
            condition = {
                "type": "simple",
                "column": tokens[i + 1],
                "operator": tokens[i + 2],
                "value": tokens[i + 3]
            }
            i += 4
            stage = 1
            continue

        if token == "GROUP":
            if stage > 1:
                raise Exception("Invalid SELECT syntax. Use: SELECT ... FROM table;")
            if i + 2 >= len(tokens) or tokens[i + 1] != "BY":
                raise Exception("Invalid SELECT syntax. Use: SELECT ... FROM table;")
            group_by = tokens[i + 2]
            i += 3
            stage = 2
            continue

        if token == "HAVING":
            if stage < 2 or stage > 2:
                raise Exception("Invalid SELECT syntax. Use: SELECT ... FROM table;")
            if i + 6 >= len(tokens):
                raise Exception("Invalid SELECT syntax. Use: SELECT ... FROM table;")

            having_agg = tokens[i + 1]
            if having_agg.upper() not in aggregates:
                raise Exception("Invalid SELECT syntax. Use: SELECT ... FROM table;")

            if tokens[i + 2] != "(":
                raise Exception("Invalid SELECT syntax. Use: SELECT ... FROM table;")

            having_col = tokens[i + 3]
            if tokens[i + 4] != ")":
                raise Exception("Invalid SELECT syntax. Use: SELECT ... FROM table;")

            having_op = tokens[i + 5]
            if having_op not in ["=", ">", "<", ">=", "<=", "!="]:
                raise Exception("Invalid SELECT syntax. Use: SELECT ... FROM table;")

            having_val = tokens[i + 6]

            having = {
                "aggregate": having_agg.upper(),
                "agg_column": having_col,
                "operator": having_op,
                "value": having_val,
            }

            i += 7
            stage = 3
            continue

        if token == "ORDER":
            if stage > 3:
                raise Exception("Invalid SELECT syntax. Use: SELECT ... FROM table;")
            if i + 2 >= len(tokens) or tokens[i + 1] != "BY":
                raise Exception("Invalid SELECT syntax. Use: SELECT ... FROM table;")

            column = tokens[i + 2]
            next_i = i + 3

            if column.upper() in aggregates and next_i < len(tokens) and tokens[next_i] == "(":
                close_paren_idx = next_i
                while close_paren_idx < len(tokens) and tokens[close_paren_idx] != ")":
                    close_paren_idx += 1

                if close_paren_idx >= len(tokens):
                    raise Exception("Invalid SELECT syntax. Use: SELECT ... FROM table;")

                agg_col = tokens[next_i + 1] if next_i + 1 < len(tokens) else "*"
                column = f"{column}({agg_col})"
                next_i = close_paren_idx + 1

            order = "ASC"
            if next_i < len(tokens) and tokens[next_i] in ["ASC", "DESC"]:
                order = tokens[next_i]
                next_i += 1

            order_by = (column, order)
            i = next_i
            stage = 4
            continue

        if token == "LIMIT":
            if stage > 4:
                raise Exception("Invalid SELECT syntax. Use: SELECT ... FROM table;")
            if i + 1 >= len(tokens):
                raise Exception("Invalid SELECT syntax. Use: SELECT ... FROM table;")
            try:
                limit = int(tokens[i + 1])
            except ValueError:
                raise Exception("Invalid SELECT syntax. Use: SELECT ... FROM table;")
            i += 2
            stage = 5
            continue

        raise Exception("Invalid SELECT syntax. Use: SELECT ... FROM table;")

    # ---------------- FINAL COMMAND ----------------
    non_agg_columns = select_columns
    if aggregate:
        non_agg_columns = [
            c for c in select_columns
            if c.upper() not in ["COUNT", "SUM", "AVG", "MIN", "MAX", "(", ")"]
            and c != agg_column
        ]
        if group_by:
            select_columns = non_agg_columns if non_agg_columns else None
        else:
            select_columns = None
    
    command = {
        "type": "SELECT",
        "table": table,
        "join": join,
        "columns": select_columns,
        "aggregate": aggregate,
        "agg_column": agg_column,
        "condition": condition,
        "group_by": group_by,
        "having": having,
        "order_by": order_by,
        "limit": limit
    }
    
    return command


def _parse_subquery(tokens, start_index):
    if start_index >= len(tokens) or tokens[start_index] != "(":
        raise Exception("Invalid subquery syntax. Use: (...)")

    end = _find_matching_paren(tokens, start_index)
    if end is None:
        raise Exception("Invalid subquery syntax - missing closing parenthesis")

    sub_tokens = tokens[start_index + 1:end]
    if not sub_tokens or sub_tokens[0] != "SELECT":
        raise Exception("Invalid subquery syntax. Only SELECT subqueries are supported")

    subquery = parse_select(sub_tokens)
    return subquery, end + 1
