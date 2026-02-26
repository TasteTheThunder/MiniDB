from storage import (
    create_table,
    insert_row,
    select_rows,
    drop_table,
    update_row,
    alter_table,
    show_tables,
    describe_table,
    truncate_table
)

# delete_row optional import
try:
    from storage import delete_row
except ImportError:
    delete_row = None


from visualizer import print_pipeline, print_trace
import config


def execute_query(command):

    # Educational Pipeline
    # if config.get_mode() == "EDUCATIONAL":
    #     print_pipeline()

    cmd_type = command["type"]


    # =========================
    # CREATE
    # =========================

    if cmd_type == "CREATE":

        print_trace(
            "EXECUTOR",
            ["Operation Identified: CREATE"]
        )

        create_table(

            command["table"],
            command["columns"],
            command.get("primary_key")

        )


    # =========================
    # INSERT
    # =========================

    elif cmd_type == "INSERT":

        print_trace(
            "EXECUTOR",
            ["Operation Identified: INSERT"]
        )

        # Handle multiple value sets (bulk insert)
        values = command["values"]
        
        # Check if it's multiple rows or single row
        if values and isinstance(values[0], list):
            # Multiple rows
            for value_set in values:
                insert_row(
                    command["table"],
                    value_set,
                    command.get("columns")
                )
        else:
            # Single row (backward compatibility)
            insert_row(
                command["table"],
                values,
                command.get("columns")
            )


    # =========================
    # SELECT
    # =========================

    elif cmd_type == "SELECT":

        print_trace(
            "EXECUTOR",
            ["Operation Identified: SELECT"]
        )

        select_rows(

            command["table"],

            command.get("condition"),

            command.get("columns"),

            command.get("aggregate"),

            command.get("agg_column"),

            command.get("group_by"),

            command.get("order_by"),

            command.get("limit")

        )


    # =========================
    # DELETE
    # =========================

    elif cmd_type == "DELETE":

        if delete_row is None:

            raise Exception(
                "DELETE not supported (delete_row missing)"
            )

        print_trace(
            "EXECUTOR",
            ["Operation Identified: DELETE"]
        )

        delete_row(

            command["table"],
            command["condition"]

        )


    # =========================
    # DROP
    # =========================

    elif cmd_type == "DROP":

        print_trace(
            "EXECUTOR",
            ["Operation Identified: DROP"]
        )

        drop_table(

            command["table"]

        )


    # =========================
    # UPDATE
    # =========================

    elif cmd_type == "UPDATE":

        print_trace(
            "EXECUTOR",
            ["Operation Identified: UPDATE"]
        )

        update_row(

            command["table"],
            command["set"],
            command["condition"]

        )

    # =========================
    # ALTER
    # =========================

    elif cmd_type == "ALTER":

        print_trace(
            "EXECUTOR",
            [f"Operation Identified: ALTER ({command['operation']})"]
        )

        alter_table(command)

    # =========================
    # SHOW TABLES
    # =========================

    elif cmd_type == "SHOW_TABLES":

        print_trace(
            "EXECUTOR",
            ["Operation Identified: SHOW TABLES"]
        )

        show_tables()

    # =========================
    # DESCRIBE
    # =========================

    elif cmd_type == "DESCRIBE":

        print_trace(
            "EXECUTOR",
            ["Operation Identified: DESCRIBE"]
        )

        describe_table(command["table"])

    # =========================
    # TRUNCATE
    # =========================

    elif cmd_type == "TRUNCATE":

        print_trace(
            "EXECUTOR",
            ["Operation Identified: TRUNCATE"]
        )

        truncate_table(command["table"])

    else:

        raise Exception(

            "Unsupported command type"

        )