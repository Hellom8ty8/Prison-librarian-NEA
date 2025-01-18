import mysql.connector as connection
from typing import Any

from sql_functions import database_functions as dbf
import config

indent_length = config.get_indent_length()
char_length = config.get_char_length()
bar = config.get_bar()
indent = config.get_indent()
cursor = config.get_cursor()
database = config.get_database()

# create animal table
print(f"\n{indent}Initiating table1...")

if not dbf.check_tbl('table1'):
    print(f"\n{indent}Table animal_details doesn't exist, creating table...")
    dbf.create_tbl('table1', ['placeholder'], ['int'])
    dbf.remove_atribute_from_tbl('table1', ['placeholder'])
    print(f"\n{indent}Table1 created succesfully")

print(f"\n{indent}Table1 initiated\n")

dbf.print_tbl('table1')

# create adopter table
print(f"\n{indent}Innitiating table2...")

if not dbf.check_tbl('table2'):
    print(f"\n{indent}Table2 doesn't exist, creating table...")
    dbf.create_tbl('table2', ['placeholder'], ['int'])
    dbf.remove_atribute_from_tbl('table2', ['placeholder'])
    print(f"\n{indent}Table2 created succesfully")

print(f"\n{indent}Table2 initiated\n")

dbf.print_tbl('table2')

print(f"{bar}\n")

dbf.print_all_tbls()
