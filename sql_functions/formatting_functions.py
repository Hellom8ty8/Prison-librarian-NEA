import mysql.connector as connection
from typing import Any

import config
indent_length = config.get_indent_length()
char_length = config.get_char_length()
bar = config.get_bar()
indent = config.get_indent()
cursor = config.get_cursor()
database = config.get_database()

print(f"\n{indent}Attempting to import formatting functions...")
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

