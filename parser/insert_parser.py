"""
Parser for INSERT INTO command
"""

def parse_insert(tokens):
    """
    Parse INSERT INTO statement
    Syntax: INSERT INTO table VALUES (val1, val2, ...)
            INSERT INTO table (col1, col2) VALUES (val1, val2)
    """
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

        column_list = [

            tokens[i]

            for i in range(col_start+1,col_end)

            if tokens[i] != ","

        ]

    # -----------------------------------
    # VALUES (...), (...), ...
    # -----------------------------------

    # Find all value sets (multiple rows support)
    values_list = []
    i = values_index + 1
    
    while i < len(tokens):
        if tokens[i] == "(":
            # Find matching closing paren
            start = i + 1
            depth = 1
            j = start
            
            while depth > 0 and j < len(tokens):
                if tokens[j] == "(":
                    depth += 1
                elif tokens[j] == ")":
                    depth -= 1
                j += 1
            
            end = j - 1
            
            # Extract values between ( and )
            values = [
                tokens[k]
                for k in range(start, end)
                if tokens[k] != ","
            ]
            values_list.append(values)
            i = j
        else:
            i += 1

    command={

        "type":"INSERT",

        "table":table,

        "values":values_list,  # Now a list of value lists

        "columns":column_list

    }
    
    return command
