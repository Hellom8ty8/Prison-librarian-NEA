import mysql.connector as connection
from typing import Any

from sql_functions import database_functions as dbf
from sql_functions import formatting_functions as ff
import config


# indent_length = config.get_indent_length()
# char_length = config.get_char_length()
bar = config.get_bar()
indent = config.get_indent()
cursor = config.get_cursor()
database = config.get_database()

# create table1
print(f"\n{ff.TermCol.cyan}{indent}Initiating table1...{ff.TermCol.ENDC}")

if not dbf.check_tbl('table1'):
    print(f"\n{ff.TermCol.orange}{indent}Table animal_details doesn't exist, creating table...{ff.TermCol.ENDC}")
    dbf.create_tbl('table1', ['placeholder'], ['int'])
    dbf.remove_atribute_from_tbl('table1', ['placeholder'])

print(f"\n{ff.TermCol.green}{indent}Table1 initiated\n{ff.TermCol.ENDC}")

dbf.print_tbl('table1')


# create table2
print(f"\n{ff.TermCol.cyan}{indent}Innitiating table2...")

if not dbf.check_tbl('table2'):
    print(f"\n{ff.TermCol.orange}{indent}Table2 doesn't exist, creating table...{ff.TermCol.ENDC}")
    dbf.create_tbl('table2', ['placeholder'], ['int'])
    dbf.remove_atribute_from_tbl('table2', ['placeholder'])

print(f"\n{ff.TermCol.green}{indent}Table2 initiated\n{ff.TermCol.ENDC}")

dbf.print_tbl('table2')

print(f"{bar}\n")

dbf.print_all_tbls()

