import mysql.connector as connection
from typing import Any

import config
indent_length = config.get_indent_length()
char_length = config.get_char_length()
bar = config.get_bar()
indent = config.get_indent()
cursor = config.get_cursor()
database = config.get_database()

class TermCol:
    # colours
    red = '\033[91m'
    orange = '\033[38;2;255;165;0m'
    yellow = '\033[93m'
    green = '\033[92m'
    blue = '\033[94m'
    pink = '\033[95m'
    purple = '\033[95m'

    cyan = '\033[96m'
    magenta = '\033[95m'

    white = '\033[97m'
    lightgrey = '\033[37m'
    grey = '\033[90m'
    darkgrey = '\033[2m'
    black = '\033[30m'

    # formatting
    header = '\033[95m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    ITALIC = '\033[3m'

print(f"\n{TermCol.yellow}{indent}Attempting to import formatting functions...{TermCol.ENDC}")
# formatting functions

def dash_number(text :str) -> int:
    dashes = (char_length) - len(text) -1
    if dashes<2: return 0
    return dashes

def print_menu(indent_text_rows :list[str], text :list[list[str]])->None:
    for indent_text, line in zip(indent_text_rows, text):
        indent_text = get_indent_text(indent_text)
        dashes = dash_number(line[0]+line[1])
        if dashes: print(f"{indent_text}{line[0]}{'-' * dashes} {line[1]}")
        else: print(f"{indent_text[0]}{line[1]}")
    print()

def get_indent_text(text :str = '') -> str:
    if type(text) == list: text = text[0]
    chars = indent_length - 5
    if len(text) < chars: return f"{' ' * (chars - len(text))}{text} --> "
    else: return(f"{text} --> ")

print(f"\n{TermCol.green}{indent}Formatting functions imported successfully!{TermCol.ENDC}")