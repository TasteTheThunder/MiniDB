"""
MiniDB Application - MySQL-like Interface
Entry point for the database system with database and table management
"""
import os
import config
from parser import parse_query
from executor import execute_query
from visualizer import print_header, print_result
from storage.database_storage import show_databases


def setup():
    """Initialize necessary directories"""
    os.makedirs("data", exist_ok=True)
    os.makedirs("metadata", exist_ok=True)


def print_welcome():
    """Print welcome banner"""
    print("\n" + "=" * 60)
    print("🚀 Welcome to NirvahaDB")
    print("=" * 60)
    print("A Lightweight Educational Relational Database System")
    print("Current Mode:", config.get_mode())
    print("Type 'HELP' for commands, or 'EXIT' to quit\n")


def print_help():
    """Print help information"""
    print("\n" + "=" * 60)
    print("📖 MiniDB - Available Commands")
    print("=" * 60)
    print("\n🗄️  DATABASE COMMANDS:")
    print("  • CREATE DATABASE <db_name>;  - Create a new database")
    print("  • DROP DATABASE <db_name>;    - Drop a database")
    print("  • SHOW DATABASES;             - List all databases")
    print("  • USE <db_name>;              - Switch to a database")
    print("\n📋 TABLE COMMANDS (requires active database):")
    print("  • CREATE TABLE <name> (...);  - Create a table")
    print("  • DROP TABLE <name>;          - Drop a table")
    print("  • SHOW TABLES;                - List tables in current database")
    print("  • DESCRIBE <table>;           - Show table structure")
    print("  • TRUNCATE TABLE <name>;      - Clear table contents")
    print("\n📝 DATA COMMANDS (requires active database):")
    print("  • INSERT INTO <table> VALUES (...);  - Insert rows")
    print("  • SELECT ... FROM <table>;           - Query data")
    print("  • UPDATE <table> SET ... WHERE ...;  - Update rows")
    print("  • DELETE FROM <table> WHERE ...;     - Delete rows")
    print("\n⚙️  OTHER COMMANDS:")
    print("  • SET MODE <mode>;      - Change mode (educational/normal)")
    print("  • SHOW MODE;             - Show current mode")
    print("  • CLEAR;                 - Clear the screen")
    print("  • HELP;                  - Show this help")
    print("  • EXIT;                  - Exit MiniDB")
    print("=" * 60 + "\n")


def get_prompt(current_db):
    """Generate prompt based on current database"""
    if current_db:
        return f"NirvahaDB [{current_db}] > "
    return "NirvahaDB > "


def main():
    """Main application loop"""
    setup()
    print_welcome()

    current_database = None

    while True:
        try:
            # Get prompt with current database context
            prompt = get_prompt(current_database)

            # Read multi-line queries
            query_lines = []

            while True:
                line = input(prompt).strip()
                query_lines.append(line)

                # Check if we should continue reading
                if (
                    line.endswith(";")
                    or line.upper() in ["EXIT", "HELP", "CLEAR"]
                    or line.upper().startswith("SET MODE")
                    or line.upper().startswith("SHOW MODE")
                ):
                    break

                # Continue prompt for multi-line queries
                prompt = "    .. > "

            # Combine all lines
            query = " ".join(query_lines).strip()

            # Handle exit
            if query.upper() == "EXIT":
                print("\n👋 Thank you for using MiniDB. Goodbye!")
                break

            # Handle help
            if query.upper() == "HELP":
                print_help()
                continue

            # Handle clear
            if query.upper().replace(" ", "") == "CLEAR;":
                os.system("cls" if os.name == "nt" else "clear")
                continue
            if query.upper().replace(" ", "") == "CLEAR":
                print("\n❌ Error: Missing semicolon. Use: CLEAR;\n")
                continue

            # Handle SET MODE
            if query.upper().startswith("SET MODE"):
                if not query.endswith(";"):
                    print("\n❌ Error: Missing semicolon. Use: SET MODE <mode>;\n")
                    continue
                mode = query.split()[-1].replace(";", "")
                config.set_mode(mode)
                print(f"✅ Mode changed to {mode.upper()}\n")
                continue

            # Handle SHOW MODE
            if query.upper().startswith("SHOW MODE"):
                if not query.endswith(";"):
                    print("\n❌ Error: Missing semicolon. Use: SHOW MODE;\n")
                    continue
                print(f"Current Mode: {config.get_mode()}\n")
                continue

            if not query.endswith(";"):
                print("\n❌ Error: Missing semicolon. End the command with ';'\n")
                continue

            # Parse query
            command = parse_query(query)

            # Block table-level commands when no database is selected
            if command.get("type") not in [
                "CREATE_DATABASE",
                "DROP_DATABASE",
                "SHOW_DATABASES",
                "USE_DATABASE",
            ]:
                if not current_database:
                    print("\n⚠️  No database selected. Use 'USE <database>;' to select a database first.")
                    print("   Or use 'CREATE DATABASE <name>;' to create a new one.\n")
                    continue

            # Execute query
            result = execute_query(command, current_database)

            # Handle USE DATABASE command (updates current database)
            if command.get("type") == "USE_DATABASE":
                current_database = result
                print_result(f"✅ Switched to database '{current_database}'")

            # Clear context if current database was dropped
            if command.get("type") == "DROP_DATABASE" and command.get("database") == current_database:
                current_database = None
                print_result("ℹ️  Current database dropped. No database selected.")

        except KeyboardInterrupt:
            print("\n\n👋 Interrupted. Goodbye!")
            break
        except Exception as e:
            print(f"\n❌ Error: {e}\n")


if __name__ == "__main__":
    main()
