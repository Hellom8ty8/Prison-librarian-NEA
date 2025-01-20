import mysql.connector as connection
from typing import Any

from . import formatting_functions as ff # need from . so that it can find the formatting_functions module
import config

indent = config.get_indent()
cursor = config.get_cursor()
database = config.get_database()

print(f"\n{ff.TermCol.yellow}{indent}Attempting to import database functions...{ff.TermCol.ENDC}")

# table creation functions -----------------------------------------------------------------------------

def create_tbl(tbl_name: str = '', columns: list[str] = [], datatypes: list[str] = []) -> int:
    print(f"\n{ff.TermCol.cyan}{indent}Attempting to create table...{ff.TermCol.ENDC}")

    # check that inputs are valid
    if not columns or not datatypes or not tbl_name:
        print(f"{ff.TermCol.red}{indent}Columns, datatypes and/or table names cannot be empty, aborting...{ff.TermCol.ENDC}")
        return 1

    if len(columns) != len(datatypes):
        print(f"{ff.TermCol.red}{indent}Column and datatype lengths do not match, aborting...{ff.TermCol.ENDC}")
        return 2

    # override option for table if it exists to stop crash
    if check_tbl(tbl_name):
        print_tbl(tbl_name)
        override: str = input(f"{ff.TermCol.yellow}{indent}Table {tbl_name} already exists, do you want to override(y/n){ff.TermCol.ENDC}")
        if override != 'y': return 0
        remove_tbl(tbl_name)

    # build sql query
    tbl: str = f"CREATE TABLE {tbl_name}("
    if len(columns) != 1:
        for column, datatype in zip(columns, datatypes):
            tbl += f"{column} {datatype}, "
        tbl = f"{tbl[:-2]});"

    else: tbl += f"{columns[0]} {str(datatypes[0])});"
    
    print(f"{ff.TermCol.cyan}{indent}Query: {tbl}{ff.TermCol.ENDC}")

    # execute query
    try:
        cursor.execute(tbl)
        add_tbl_row_id_column(tbl_name)
        database.commit()
        print(f"{ff.TermCol.green}{indent}Successfully made table {tbl_name}{ff.TermCol.ENDC}")

    except Exception as e:
        print(f"{ff.TermCol.red}{indent}Error creating table: {e}{ff.TermCol.ENDC}")
        return 3
    
    print(f"{ff.TermCol.green}{indent}Table {tbl_name} has been successfully created{ff.TermCol.ENDC}")
    
    return 0

def remove_tbl(tbl_name: str) -> int:
    print(f"\n{ff.TermCol.cyan}{indent}Attempting to remove table...{ff.TermCol.ENDC}")

    if not check_tbl(tbl_name):
        print(f"{ff.TermCol.red}{indent}{tbl_name} does not exist, aborting...{ff.TermCol.ENDC}")
        return 1

    sql = f"DROP TABLE {tbl_name};"
    print(f"{ff.TermCol.cyan}{indent}Query: {sql}{ff.TermCol.ENDC}")

    try:
        cursor.execute(sql)
        database.commit()
        print(f"{ff.TermCol.green}{indent}Table {tbl_name} has been successfully removed{ff.TermCol.ENDC}")

    except Exception as e:
        print(f"{ff.TermCol.red}{indent}Error removing table: {e}{ff.TermCol.ENDC}")
        return 3
    
    print(f"{ff.TermCol.green}{indent}Table {tbl_name} has been successfully removed{ff.TermCol.ENDC}")
    
    return 0

def remove_all_tbls() -> int:
    print(f"{ff.TermCol.cyan}{indent}Attempting to remove all tables...{ff.TermCol.ENDC}")

    try: tables = get_tbl_names()
    
    except Exception as e:
        print(f"{ff.TermCol.red}{indent}Error fetching table names: {e}{ff.TermCol.ENDC}")
        return 3

    if not tables:
        print(f"{ff.TermCol.yellow}{indent}No tables exist to remove.{ff.TermCol.ENDC}")
        return 1

    sql = f"DROP TABLE {table};"
    print(f"{ff.TermCol.cyan}{indent}Query: {sql}{ff.TermCol.ENDC}")

    for table in tables:
        try:
            cursor.execute(sql)
            database.commit()
            print(f"{ff.TermCol.green}{indent}Table {table} removed successfully{ff.TermCol.ENDC}")
        
        except Exception as e:
            print(f"{ff.TermCol.red}{indent}Error removing table {table}: {e}{ff.TermCol.ENDC}")
            return 3

    print(f"{ff.TermCol.green}{indent}All tables have been successfully removed{ff.TermCol.ENDC}")
    
    return 0

# table manipulating functions -------------------------------------------------------------------------
def insert_atribute_to_tbl(tbl_name: str, new_columns: list[str], datatypes: list[str]) -> int:
    print(f"\n{ff.TermCol.cyan}{indent}Attempting to create attribute...{ff.TermCol.ENDC}")

    if not tbl_name or not new_columns or not datatypes:
        print(f"{ff.TermCol.red}{indent}Missing required inputs, aborting...{ff.TermCol.ENDC}")
        return 1

    if len(new_columns) != len(datatypes):
        print(f"{ff.TermCol.red}{indent}Column and datatype lengths do not match, aborting...{ff.TermCol.ENDC}")
        return 2

    new_columns_str = ", ".join(f"{column} {datatype}" for column, datatype in zip(new_columns, datatypes))
    sql = "ALTER TABLE {tbl_name} ADD ({new_columns_str});"
    print(f"{ff.TermCol.cyan}{indent}Query: {sql}{ff.TermCol.ENDC}")

    try:
        cursor.execute(sql)
        database.commit()

    except Exception as e:
        print(f"{ff.TermCol.red}{indent}Error adding attributes: {e}{ff.TermCol.ENDC}")
        return 3
    
    print(f"{ff.TermCol.green}{indent}Successfully added {', '.join(new_columns)} to {tbl_name}{ff.TermCol.ENDC}")

    return 0

def insert_records_to_tbl(tbl_name: str = '', num_rows: int = 0) -> int:
    print(f"\n{ff.TermCol.cyan}{indent}Attempting to create record...{ff.TermCol.ENDC}")
    
    if not tbl_name or not num_rows:
        print(f"{ff.TermCol.red}{indent}Invalid table name or row count, aborting...{ff.TermCol.ENDC}")
        return 1
    
    if num_rows <= 0 or type(num_rows) != int:
        print(f"{ff.TermCol.red}{indent}Invalid row count, aborting...{ff.TermCol.ENDC}")
        return 2

    if not check_tbl(tbl_name):
        override = input(f"{ff.TermCol.yellow}{indent}Table {tbl_name} doesn't exist. Create it? (y/n): {ff.TermCol.ENDC}")
        if override.lower() != 'y': return 0
        create_tbl(tbl_name, [], [])
        add_tbl_row_id_column(tbl_name)

    try:
        cursor.execute(f"DESCRIBE {tbl_name}")
        columns_info = cursor.fetchall()

    except Exception as e:
        print(f"{ff.TermCol.red}{indent}Error describing table: {e}{ff.TermCol.ENDC}")
        return 3

    if not columns_info:
        print(f"{ff.TermCol.red}{indent}Table {tbl_name} has no columns, aborting...{ff.TermCol.ENDC}")
        return 1

    new_columns = [column[0] for column in columns_info]
    datatypes = ['%s' for _ in new_columns]

    data = [(None,) * len(new_columns) for _ in range(num_rows)]
    sql = f"INSERT INTO {tbl_name} ({', '.join(new_columns)}) VALUES ({', '.join(datatypes)})"
    print(f"{ff.TermCol.cyan}{indent}Query: {sql}{ff.TermCol.ENDC}")

    try:
        cursor.executemany(sql, data)
        database.commit()
        print(f"{ff.TermCol.green}{indent}Successfully added {num_rows} rows to {tbl_name}.{ff.TermCol.ENDC}")
        
    except Exception as e:
        print(f"{ff.TermCol.red}{indent}Error inserting data: {e}{ff.TermCol.ENDC}")
        return 3

    print(f"{ff.TermCol.green}{indent}Successfully added {num_rows} rows to {tbl_name}.{ff.TermCol.ENDC}")
    
    return 0

def remove_atribute_from_tbl(tbl_name: str, removed_columns: list[str]) -> int:
    print(f"{ff.TermCol.cyan}{indent}Attempting to remove attribute(s)...{ff.TermCol.ENDC}")

    if not tbl_name or not removed_columns:
        print(f"{ff.TermCol.red}{indent}Missing Table name and/or removed collumns, aborting...{ff.TermCol.ENDC}")
        return 1

    if not check_tbl(tbl_name):
        print(f"{ff.TermCol.red}{indent}Table {tbl_name} doesn't exist, aborting...{ff.TermCol.ENDC}")
        return 2

    sql = f"ALTER TABLE {tbl_name} DROP COLUMN {column};"
    print(f"{ff.TermCol.cyan}{indent}Query: {sql}{ff.TermCol.ENDC}")

    for column in removed_columns:
        try: cursor.execute(sql)

        except Exception as e:
            print(f"{ff.TermCol.red}{indent}Error removing column {column}: {e}{ff.TermCol.ENDC}")
            return 3

    database.commit()
    
    print(f"{ff.TermCol.green}{indent}Successfully removed {', '.join(removed_columns)} from {tbl_name}{ff.TermCol.ENDC}")
    
    return 0

def remove_record_from_tbl(tbl_name: str, row_ids: str) -> int:
    print(f"\n{ff.TermCol.cyan}{indent}Attempting to remove records...{ff.TermCol.ENDC}")

    if not tbl_name or not row_ids:
        print(f"{ff.TermCol.red}{indent}Missing table name or row IDs, aborting...{ff.TermCol.ENDC}")
        return 1

    if not check_tbl(tbl_name):
        print(f"{ff.TermCol.red}{indent}Table {tbl_name} doesn't exist, aborting...{ff.TermCol.ENDC}")
        return 1

    try: id_list = [int(row_id.strip()) for row_id in row_ids.split(',')]
   
    except ValueError:
        print(f"{ff.TermCol.red}{indent}Invalid row ID format. Please provide a comma-separated list of integers.{ff.TermCol.ENDC}")
        return 2

    if not id_list:
        print(f"{ff.TermCol.red}{indent}No valid row IDs provided, aborting...{ff.TermCol.ENDC}")
        return 2

    placeholders = ', '.join(['%s'] * len(id_list))
    sql = f"DELETE FROM {tbl_name} WHERE id IN ({placeholders})"
    print(f"{ff.TermCol.cyan}{indent}Query: {sql}{ff.TermCol.ENDC}")

    try:
        cursor.execute(sql, id_list)
        database.commit()
        print(f"{ff.TermCol.green}{indent}Successfully removed records from {tbl_name}.{ff.TermCol.ENDC}")

    except Exception as e:
        print(f"{ff.TermCol.red}{indent}Error removing records: {e}{ff.TermCol.ENDC}")
        return 3
    
    print(f"\n{ff.TermCol.green}{indent}Successfully imported database functions{ff.TermCol.ENDC}")

    return 0

# data functions ---------------------------------------------------------------------------------------

def insert_data_into_tbl_records(tbl_name: str = '', row_ids: list[int] = [], row_data: list[list[str]] = []) -> int:
    print(f"\n{ff.TermCol.cyan}{indent}Attempting to insert data into records...{ff.TermCol.ENDC}")

    if not tbl_name:
        print(f"{indent}No table name provided, aborting...")
        return 1

    if not check_tbl(tbl_name):
        print(f"{indent}Table {tbl_name} doesn't exist, aborting...")
        return 1

    if not row_ids:
        print(f"{indent}No row IDs provided, aborting...")
        return 1

    if not row_data:
        print(f"{indent}No row data provided, aborting...")
        return 1

    if len(row_data) != len(row_ids):
        print(f"{indent}Number of rows does not match the data, aborting...")
        return 1

    cursor.execute(
        f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = %s ORDER BY ORDINAL_POSITION;",
        (tbl_name,)
    )
    columns: list[str] = [column[0] for column in cursor.fetchall()]

    if "id" in columns:
        columns.remove("id")

    if any(len(row) != len(columns) for row in row_data):
        print(f"{indent}Row data does not match the number of columns, aborting...")
        return 1

    cursor.execute(f"SELECT id FROM {tbl_name}")
    existing_ids = {row[0] for row in cursor.fetchall()}  # Fetch all existing row IDs
    missing_ids = [row_id for row_id in row_ids if row_id not in existing_ids]

    if missing_ids:
        print(f"{indent}The following row IDs do not exist in {tbl_name}: {missing_ids}")
        valid_indices = [i for i, row_id in enumerate(row_ids) if row_id in existing_ids]
        row_ids = [row_ids[i] for i in valid_indices]
        row_data = [row_data[i] for i in valid_indices]

    for index, row_id in enumerate(row_ids):
        updates = []
        values = []
        for column, value in zip(columns, row_data[index]):
            updates.append(f"{column} = %s")
            values.append(value)

        sql = f"UPDATE {tbl_name} SET {', '.join(updates)} WHERE id = %s"
        print(f"{indent}Query: {sql}")
        
        values.append(str(row_id))
        cursor.execute(sql, values)

    database.commit()
    
    print(f"{indent}Successfully updated data in rows")
    
    return 0

def insert_data_into_attributes(tbl_name: str = '', attributes: list[str] = [''], data: list[list] = []) -> int:
    transposed_data = list(zip(*data))

    if not tbl_name:
        print(f"{ff.TermCol.red}{indent}No table name inputed, aborting...{ff.TermCol.ENDC}")
        return 1
    
    if not check_tbl(tbl_name):
        print(f"{ff.TermCol.red}{indent}Table {tbl_name} doesn't exist, aborting...{ff.TermCol.ENDC}")
        return 2

    if not attributes or attributes == ['']:
        print(f"{ff.TermCol.red}{indent}No attributes provided, aborting...{ff.TermCol.ENDC}")
        return 1

    sql = f"DELETE FROM {tbl_name}"
    print(f"{ff.TermCol.cyan}{indent}Query: {sql}{ff.TermCol.ENDC}")

    try:
        cursor.execute(sql)
        database.commit()

    except Exception as e:
        print(f"{ff.TermCol.red}{indent}Error deleting data: {e}{ff.TermCol.ENDC}")
        return 3

    sql = f"INSERT INTO {tbl_name} ({', '.join(attributes)}) VALUES ({', '.join(['%s' for _ in attributes])})"

    cursor.executemany(sql, transposed_data)
    database.commit()
    
    print(f"{ff.TermCol.green}{indent}Successfully inserted data into {tbl_name}.{ff.TermCol.ENDC}")

    reset_tbl_row_ids(tbl_name)

# print functions --------------------------------------------------------------------------------------

def print_all_tbl_names() -> int:   
    tbl_names = get_tbl_names()
    tbl_id = len(tbl_names)
    
    if not tbl_id:
        print(f"{ff.TermCol.red}{indent}No tables found, aborting...{ff.TermCol.ENDC}")
        return 1
    
    for i, table in zip(range(tbl_id), tbl_names):

        dashes = ff.dash_number(f"{table} {i}")
        if dashes: print(f"""{indent}{str(table)} {'-' * (dashes)} {i}{indent}""")
        else: print(f"""{indent}{str(table)} {'-' * (dashes)}{i}{indent}""")
    
    return 0

def print_tbl(tbl_name :str, printed_columns: str = '*', params: str = '') -> int:

    if not check_tbl(tbl_name):
        print(f"\n{ff.TermCol.red}{indent}{tbl_name} doesn't exist, aborting...{ff.TermCol.ENDC}")
        return 1

    sql1 = f"SELECT {printed_columns} FROM {tbl_name};"
    if params != '':
        sql1 += f" WHERE {params}"

    cursor.execute(sql1)
    data = cursor.fetchall()

    sql = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tbl_name}' "
    if params == '': sql += f"ORDER BY ORDINAL_POSITION;"
    else: sql += f"{params} ORDER BY ORDINAL_POSITION;"
    
    cursor.execute(sql) 

    column_names = [col[0] for col in cursor.fetchall()]
    if "id" in column_names:
        id_pos :int = column_names.index("id")
        column_names = ["id"] + column_names[:id_pos] + column_names[id_pos+1:]

    print(f"{indent}id /", column_names[1:])
    for row in data:
        row_id = row[id_pos]
        row_data = [value for i, value in enumerate(row) if i != id_pos]
        print(f"{indent}{row_id} /", row_data)

    print()

    return 0

def print_all_tbls(printed_columns: str = '*', params: str = '') -> int:
    tables = get_tbl_names()
    for table in tables:
        err_code = print_tbl(table, printed_columns, params)
        if err_code:
            return err_code

    return 0
# misc functions ---------------------------------------------------------------------------------------

def check_tbl(tbl_name :str = '') -> bool:

    # remove table if it exists to stop crash
    if tbl_name == '':
        return 1
    cursor.execute(f"SHOW TABLES LIKE '{tbl_name}';")
    return cursor.fetchall() != []

def add_tbl_row_id_column(tbl_name: str = '') -> int:
    print(f"\n{ff.TermCol.cyan}{indent}Adding id column...{ff.TermCol.ENDC}")

    if not check_tbl(tbl_name):
        print(f"{ff.TermCol.red}{indent}{tbl_name} doesn't exist, aborting...{ff.TermCol.ENDC}")
        return 1

    try:
        cursor.execute(f"DESCRIBE {tbl_name}")
        column_names: list = [column[0] for column in cursor.fetchall()]

    except Exception as e:
        print(f"{ff.TermCol.red}{indent}Error describing table: {e}{ff.TermCol.ENDC}")
        return 3

    if "id" not in column_names:
        try:
            cursor.execute(f"ALTER TABLE {tbl_name} ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY;")
            database.commit()
            print(f"{ff.TermCol.green}{indent}Added id column to the table {tbl_name}{ff.TermCol.ENDC}")

        except Exception as e:
            print(f"{ff.TermCol.red}{indent}Error adding id column: {e}{ff.TermCol.ENDC}")
            return 3

    print(f"{ff.TermCol.green}{indent}Successfully added id column to {tbl_name}.{ff.TermCol.ENDC}")

    return 0

def reset_tbl_row_ids(tbl_name: str, id_column: str = "id") -> int:
    print(f"\n{ff.TermCol.cyan}{indent}Fixing id column for {tbl_name}...{ff.TermCol.ENDC}")
    
    if not tbl_name:
        print(f"{ff.TermCol.red}{indent}No table name provided, aborting...{ff.TermCol.ENDC}")
        return 1

    if not check_tbl(tbl_name):
        print(f"{ff.TermCol.red}{indent}{tbl_name} doesn't exist, aborting...{ff.TermCol.ENDC}")
        return 2

    try:
        sql1 = f"CREATE TEMPORARY TABLE temp_ids AS SELECT ROW_NUMBER() OVER (ORDER BY {id_column}) AS {id_column}, {id_column} AS old_id FROM {tbl_name};"
        print(f"{ff.TermCol.cyan}{indent}Query: {sql1}{ff.TermCol.ENDC}")
        cursor.execute(sql1)
        
        sql2 = f"UPDATE {tbl_name} AS original JOIN temp_ids AS temp ON original.{id_column} = temp.old_id SET original.{id_column} = temp.{id_column};"
        print(f"{ff.TermCol.cyan}{indent}Query: {sql2}{ff.TermCol.ENDC}")
        cursor.execute(sql2)

        sql3 = f"DROP TEMPORARY TABLE temp_ids;"
        print(f"{ff.TermCol.cyan}{indent}Query: {sql3}{ff.TermCol.ENDC}")
        cursor.execute(sql3)

        database.commit()
        print(f"{ff.TermCol.green}{indent}Successfully reset row IDs for {tbl_name}.{ff.TermCol.ENDC}")

    except Exception as e:
        print(f"{ff.TermCol.red}{indent}Error while fixing row IDs: {e}{ff.TermCol.ENDC}")
        return 3
    
    print(f"{ff.TermCol.green}{indent}Successfully reset row IDs for {tbl_name}.{ff.TermCol.ENDC}")

    return 0

def get_tbl_names() -> list[str]:
    try:
        cursor.execute("SHOW TABLES")
        tbl_names = [tbl_name[0] for tbl_name in cursor.fetchall()]
        return tbl_names

    except Exception as e:
        print(f"{ff.TermCol.red}{indent}Error fetching table names: {e}{ff.TermCol.ENDC}")
        return 3

    return []

print(f"\n{ff.TermCol.green}{indent}Successfully imported database functions{ff.TermCol.ENDC}")