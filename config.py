# Default mode
MODE = "EDUCATIONAL"

def set_mode(new_mode):
    global MODE
    MODE = new_mode.upper()

def get_mode():
    return MODE
