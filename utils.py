import config
import os

def edu_print(message=""):
    if config.get_mode() == "EDUCATIONAL":
        print(message)

def stage(title):
    if config.get_mode() == "EDUCATIONAL":
        print("\n" + "─" * 50)
        print(f"🔹 {title}")
        print("─" * 50)

def header(query):
    if config.get_mode() == "EDUCATIONAL":
        print("\n" + "=" * 60)
        print("🎓 MiniDB Execution")
        print("=" * 60)
        print("📝 Query:")
        print(" ", query.strip())


# ===============================
# DB PATH HELPERS
# ===============================

DATA_DIR = "data"
META_DIR = "metadata"


def table_paths(table, database=None):
    """
    Get paths for table data and metadata files.
    If database is specified, returns database-specific paths.
    If database is None, returns legacy paths (for backward compatibility).
    """
    if database:
        return (
            os.path.join(DATA_DIR, database, table + ".tbl"),
            os.path.join(META_DIR, database, table + ".meta")
        )
    else:
        return (
            os.path.join(DATA_DIR, table + ".tbl"),
            os.path.join(META_DIR, table + ".meta")
        )


def check_table_exists(tbl, meta):

    if not os.path.exists(tbl) or not os.path.exists(meta):

        raise Exception("Table does not exist")


# ===============================
# STRING HELPERS
# ===============================

def remove_quotes(value):

    value = value.strip()

    if (

        (value.startswith('"') and value.endswith('"'))

        or

        (value.startswith("'") and value.endswith("'"))

    ):

        return value[1:-1]

    return value


# ===============================
# COMPARISON HELPER
# ===============================

def compare(cell, op, val):

    val = remove_quotes(val)

    try:

        cell_num = float(cell)
        val_num = float(val)

        if op == "=":
            return cell_num == val_num

        if op == ">":
            return cell_num > val_num

        if op == "<":
            return cell_num < val_num

        if op == ">=":
            return cell_num >= val_num

        if op == "<=":
            return cell_num <= val_num

        if op == "!=":
            return cell_num != val_num

    except:

        if op == "=":
            return cell == val

        if op == ">=":
            return cell >= val

        if op == "<=":
            return cell <= val

        if op == "!=":
            return cell != val

    return False

# ===============================
# DATATYPE VALIDATION
# ===============================

SUPPORTED_TYPES = [

    "INT",
    "DOUBLE",
    "CHAR",
    "VARCHAR"

]


def validate_value(value, datatype):

    datatype = datatype.upper()

    value = value.strip()

    # ---------- INT ----------

    if datatype == "INT":

        if not value.isdigit():

            raise Exception(

                f"Invalid INT value : {value}"

            )

        return


    # ---------- DOUBLE ----------

    elif datatype == "DOUBLE":

        try:

            float(value)

        except:

            raise Exception(

                f"Invalid DOUBLE value : {value}"

            )

        return


    # ---------- CHAR / VARCHAR ----------

    elif datatype in ["CHAR", "VARCHAR"]:

        if not (

            (value.startswith('"') and value.endswith('"'))

            or

            (value.startswith("'") and value.endswith("'"))

        ):

            raise Exception(

                f"{datatype} value must be inside quotes"

            )

        return


    else:

        raise Exception(

            f"Unsupported datatype {datatype}"

        )