"""
Parser for INSERT INTO command
"""

def parse_insert(tokens):
    """
    Parse INSERT INTO statement
    Syntax: INSERT INTO table VALUES (val1, val2, ...)
            INSERT INTO table (col1, col2) VALUES (val1, val2)
    """
    if len(tokens) < 4:
        raise Exception("Invalid INSERT syntax. Use: INSERT INTO table VALUES (...);")

    if tokens[0] != "INSERT" or tokens[1] != "INTO":
        raise Exception("Invalid INSERT syntax. Use: INSERT INTO table VALUES (...);")

    table = tokens[2]

    if "VALUES" not in tokens:

        raise Exception(

            "Invalid INSERT syntax"

        )

    values_index = tokens.index("VALUES")

    # -----------------------------------
    # COLUMN LIST (OPTIONAL)
    # -----------------------------------

    column_list = None

    # INSERT INTO students (id,name)

    if tokens[3] == "(":

        col_start = 3

        col_end = tokens.index(")",col_start)

        if col_end + 1 != values_index:
            raise Exception("Invalid INSERT syntax. Use: INSERT INTO table (col1, col2) VALUES (...);")

        column_list = [

            tokens[i]

            for i in range(col_start+1,col_end)

            if tokens[i] != ","

        ]

    elif values_index != 3:
        raise Exception("Invalid INSERT syntax. Use: INSERT INTO table VALUES (...);")

    # -----------------------------------
    # VALUES (...), (...), ...
    # -----------------------------------

    # Find all value sets (multiple rows support)
    values_list = []
    i = values_index + 1

    if i >= len(tokens):
        raise Exception("Invalid INSERT syntax. Use: INSERT INTO table VALUES (...);")

    expect_value_set = True

    while i < len(tokens):
        if expect_value_set:
            if tokens[i] != "(":
                raise Exception("Invalid INSERT syntax. Use: INSERT INTO table VALUES (...);")

            start = i + 1
            depth = 1
            j = start

            while depth > 0 and j < len(tokens):
                if tokens[j] == "(":
                    depth += 1
                elif tokens[j] == ")":
                    depth -= 1
                j += 1

            if depth != 0:
                raise Exception("Invalid INSERT syntax. Use: INSERT INTO table VALUES (...);")

            end = j - 1
            values = [
                tokens[k]
                for k in range(start, end)
                if tokens[k] != ","
            ]

            if not values:
                raise Exception("Invalid INSERT syntax. Use: INSERT INTO table VALUES (...);")

            values_list.append(values)
            i = j
            expect_value_set = False
        else:
            if tokens[i] == ",":
                expect_value_set = True
                i += 1
            else:
                raise Exception("Invalid INSERT syntax. Use: INSERT INTO table VALUES (...);")

    if expect_value_set:
        raise Exception("Invalid INSERT syntax. Use: INSERT INTO table VALUES (...);")

    command={

        "type":"INSERT",

        "table":table,

        "values":values_list,  # Now a list of value lists

        "columns":column_list

    }
    
    return command
