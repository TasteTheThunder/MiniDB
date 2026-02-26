import os
import config
from parser import parse_query
from executor import execute_query
from visualizer import print_header


def setup():
    os.makedirs("data", exist_ok=True)
    os.makedirs("metadata", exist_ok=True)


def main():
    setup()

    print("🎓 Welcome to MiniDB")
    print("Current Mode:", config.get_mode())
    print("Type 'exit' to quit\n")

    while True:
        # Read multi-line queries (continue until semicolon)
        query_lines = []
        prompt = "MiniDB > "
        
        while True:
            line = input(prompt).strip()
            query_lines.append(line)
            
            # Check if the line ends with semicolon or is a special command
            if line.endswith(";") or line.lower() == "exit" or line.upper().startswith("SET MODE") or line.upper().startswith("SHOW MODE"):
                break
            
            # Continue with continuation prompt
            prompt = "    .. > "
        
        # Combine all lines into a single query
        query = " ".join(query_lines).strip()

        if query.lower() == "exit":
            break

        # 🔹 SET MODE
        if query.upper().startswith("SET MODE"):
            mode = query.split()[-1].replace(";", "")
            config.set_mode(mode)
            print(f"🔄 Mode changed to {mode.upper()}")
            continue

        # 🔹 SHOW MODE
        if query.upper().startswith("SHOW MODE"):
            print("Current Mode:", config.get_mode())
            continue

        try:
            # Use new visual header
            print_header(query)

            command = parse_query(query)
            execute_query(command)

        except Exception as e:
            print("❌ Error:", e)


if __name__ == "__main__":
    main()
