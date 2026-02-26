import config

WIDTH = 70


# ================= COLORS =================

RESET = "\033[0m"

BLUE = "\033[94m"
CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
MAGENTA = "\033[95m"
GRAY = "\033[90m"


ICONS = {

    "TOKENIZER": ("🔎", CYAN),
    "PARSER": ("🧠", BLUE),
    "EXECUTOR": ("⚙️", YELLOW),
    "STORAGE ENGINE": ("💾", MAGENTA),
    "FILE SYSTEM": ("📂", GREEN)

}


# =====================================================
# HEADER
# =====================================================

def print_header(query):

    if config.get_mode() != "EDUCATIONAL":
        return

    title = "MINIDB QUERY EXECUTION VISUALIZER"

    pad = (WIDTH - len(title)) // 2

    print("\n" + CYAN + "="*WIDTH)

    print(" "*pad + title)

    print("="*WIDTH + RESET)

    print("\n" + YELLOW + "QUERY:" + RESET, query.strip())
    print(GRAY + "-"*WIDTH + RESET + "\n")


# =====================================================
# PIPELINE DISABLED
# =====================================================

def print_pipeline():
    return


# =====================================================
# TRACE SECTIONS
# =====================================================

def print_trace(section, lines):

    if config.get_mode() != "EDUCATIONAL":
        return

    icon, color = ICONS.get(section, ("➡️", RESET))

    print(color + f"{icon} {section}")
    print("─"*WIDTH + RESET)

    for line in lines:

        print(f"   {GRAY}➤{RESET} {line}")

    print()


# =====================================================
# FINAL RESULT
# =====================================================

def print_result(message):

    if config.get_mode() != "EDUCATIONAL":
        return

    title = "EXECUTION COMPLETE"

    pad = (WIDTH - len(title)) // 2

    print(GREEN + "="*WIDTH)

    print(" "*pad + title)

    print("="*WIDTH + RESET)

    print(message)

    print(GREEN + "="*WIDTH + RESET + "\n")