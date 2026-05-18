from storage import (
    create_table,
    insert_row,
    select_rows,
    update_row,
    delete_row,
    drop_table,
    alter_table,
    show_tables,
    describe_table,
    truncate_table
)

from storage.database_storage import (
    create_database,
    drop_database,
    show_databases,
    database_exists
)

from visualizer import print_pipeline, print_trace, print_result
import config
from normalization.schema_analyzer import analyze_schema


def execute_query(command, database=None):

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
            command.get("primary_key"),
            command.get("foreign_keys"),
            database=database
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
                    command.get("columns"),
                    database=database
                )
        else:
            # Single row (backward compatibility)
            insert_row(
                command["table"],
                values,
                command.get("columns"),
                database=database
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
            command.get("having"),
            command.get("order_by"),
            command.get("limit"),
            database=database,
            join=command.get("join")
        )


    # =========================
    # DELETE
    # =========================

    elif cmd_type == "DELETE":

        print_trace(
            "EXECUTOR",
            ["Operation Identified: DELETE"]
        )

        delete_row(
            command["table"],
            command["condition"],
            database=database
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
            command["table"],
            database=database
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
            command["condition"],
            database=database
        )

    # =========================
    # ALTER
    # =========================

    elif cmd_type == "ALTER":

        print_trace(
            "EXECUTOR",
            [f"Operation Identified: ALTER ({command['operation']})"]
        )

        alter_table(command, database=database)

    # =========================
    # SHOW TABLES
    # =========================

    elif cmd_type == "SHOW_TABLES":

        print_trace(
            "EXECUTOR",
            ["Operation Identified: SHOW TABLES"]
        )

        show_tables(database=database)

    # =========================
    # DESCRIBE
    # =========================

    elif cmd_type == "DESCRIBE":

        print_trace(
            "EXECUTOR",
            ["Operation Identified: DESCRIBE"]
        )

        describe_table(command, database=database)

    # =========================
    # TRUNCATE
    # =========================

    elif cmd_type == "TRUNCATE":

        print_trace(
            "EXECUTOR",
            ["Operation Identified: TRUNCATE"]
        )

        truncate_table(command, database=database)

    # =========================
    # ANALYZE SCHEMA
    # =========================

    elif cmd_type == "ANALYZE_SCHEMA":
        analyze_schema(command["table"], interactive=True, database=database)

    # =========================
    # DATABASE COMMANDS
    # =========================

    elif cmd_type == "CREATE_DATABASE":
        print_trace(
            "EXECUTOR",
            ["Operation Identified: CREATE DATABASE"]
        )
        create_database(command["database"])
        print_result(f"Database '{command['database']}' created successfully")

    elif cmd_type == "DROP_DATABASE":
        print_trace(
            "EXECUTOR",
            ["Operation Identified: DROP DATABASE"]
        )
        drop_database(command["database"])
        print_result(f"Database '{command['database']}' dropped successfully")

    elif cmd_type == "SHOW_DATABASES":
        print_trace(
            "EXECUTOR",
            ["Operation Identified: SHOW DATABASES"]
        )
        databases = show_databases()
        if not databases:
            print_result("No databases found")
        else:
            print("\n📊 Available Databases:")
            for db in sorted(databases):
                print(f"  • {db}")
            print()

    elif cmd_type == "USE_DATABASE":
        print_trace(
            "EXECUTOR",
            ["Operation Identified: USE DATABASE"]
        )
        db_name = command["database"]
        if not database_exists(db_name):
            raise Exception(f"Database '{db_name}' does not exist")
        # Return the database name - app.py will handle setting it
        return db_name

    else:

        raise Exception(

            "Unsupported command type"

        )