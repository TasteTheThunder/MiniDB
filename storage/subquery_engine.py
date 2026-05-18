"""
Subquery execution helpers for NirvahaDB.
"""
from visualizer import print_trace
from utils import remove_quotes


def execute_subquery(subquery_command, database=None):
    from storage.select_storage import select_rows

    rows, columns = select_rows(
        subquery_command["table"],
        subquery_command.get("condition"),
        subquery_command.get("columns"),
        subquery_command.get("aggregate"),
        subquery_command.get("agg_column"),
        subquery_command.get("group_by"),
        subquery_command.get("having"),
        subquery_command.get("order_by"),
        subquery_command.get("limit"),
        database=database,
        join=subquery_command.get("join"),
        return_rows=True
    )

    print_trace("SUBQUERY", [
        f"Subquery Result Rows : {len(rows)}",
        f"Subquery Columns : {', '.join(columns) if columns else 'None'}"
    ])

    return rows, columns


def extract_in_values(rows):
    values = []
    for row in rows:
        if not row:
            continue
        values.append(remove_quotes(str(row[0])))
    return values


def ensure_non_correlated(subquery_command):
    condition = subquery_command.get("condition")
    if not condition:
        return

    cond_type = condition.get("type") if isinstance(condition, dict) else "simple"
    if cond_type != "simple":
        return

    if isinstance(condition, dict):
        column = condition.get("column", "")
        value = condition.get("value", "")
    else:
        column, _op, value = condition

    if "." in str(column) or "." in str(value):
        raise Exception(
            "Correlated subqueries are not supported for EXISTS/NOT EXISTS"
        )
