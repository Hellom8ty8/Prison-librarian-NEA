import mysql.connector as connection
from typing import Any

from . import formatting_functions as ff
import config

indent = config.get_indent()
cursor = config.get_cursor()
database = config.get_database()

print(f"\n{indent}Attempting to import database functions...")

# table creation functions -----------------------------------------------------------------------------

def create_tbl(tbl_name :str = '', columns :list[str] = [], datatypes :list[str] = []) -> None:
    print(f"\n{indent}Creating table...")

    # check that inputs are valid
    if not (columns or datatypes): print(f"{indent}Columns and/or datatypes cannot be empty, aborting...")
    if tbl_name == '':
        print(f"{indent}No table selected, aborting...")
        return None

    # overide option for table if it exists to stop crash
    if check_tbl(tbl_name):
        print_tbl(tbl_name)
        override :str= input(f"{indent}Table {tbl_name} already exists, do you want to override(y/n)")
        if override != 'y': return None
        remove_tbl(tbl_name)


    # build sql query
    tbl :str= f"CREATE TABLE {tbl_name}("
    if len(columns) != 1:
        for column, datatype in zip(columns, datatypes):
            tbl += f"{column} {datatype}, "
        tbl = f"{tbl[:-2]});"
    else: tbl += f"{columns[0]} {str(datatypes[0])});"
    print(f"{indent}Querie: {tbl}")

    # execute query
    cursor.execute(tbl)
    add_tbl_row_id_column(tbl_name)
    database.commit()
    print(f"{indent}Successfully made table {tbl_name}")

def remove_tbl(tbl_name :str) -> None:

    print(f"\n{indent}Removing table...")
    
    if check_tbl(tbl_name):
        cursor.execute(f"DROP TABLE {tbl_name};")
        database.commit()
        print(f"{indent}Table {tbl_name} has been succesfully removed")
        return None
    
    print(f"{indent}{tbl_name} Does not exist, aborting...")

def remove_all_tbls() -> None:
    print(f"{indent}Attempting to remove all tables...")
    tables = get_tbl_names()
    for table in tables:
        print(f"\n{indent}Removing table {table}...")
        cursor.execute(f"DROP TABLE {table};")
        database.commit()
        print(f"\n{indent}Table {table} removed succesfully")
    print(f"{indent}All tables have been succesfully removed")

# table manipulating functions -------------------------------------------------------------------------

def insert_atribute_to_tbl(tbl_name: str, new_columns: list[str], datatypes: list[str]) -> None:
    print(f"\n{indent}Attempting to create atribute...")

    if not tbl_name or not new_columns or not datatypes:
        print(f"{indent}Missing required inputs, aborting...")
        return

    if len(new_columns) != len(datatypes):
        print(f"{indent}Column and datatype lengths do not match, aborting...")
        return

    new_columns_str = ", ".join(f"{column} {datatype}" for column, datatype in zip(new_columns, datatypes))

    try:
        cursor.execute(f"ALTER TABLE {tbl_name} ADD ({new_columns_str});")
        database.commit()
        print(f"{indent}Successfully added {', '.join(new_columns)} to {tbl_name}")
    except Exception as e:
        print(f"{indent}Error adding attributes: {e}")

def insert_records_to_tbl(tbl_name: str = '', num_rows: int = 0) -> None:
    if not tbl_name or num_rows <= 0:
        print(f"{indent}Invalid table name or row count, aborting...")
        return

    if not check_tbl(tbl_name):
        override = input(f"Table {tbl_name} doesn't exist. Create it? (y/n): ")
        if override.lower() != 'y':
            return
        create_tbl(tbl_name, [], [])
        add_tbl_row_id_column(tbl_name)

    cursor.execute(f"DESCRIBE {tbl_name}")
    columns_info = cursor.fetchall()
    if not columns_info:
        print(f"{indent}Table {tbl_name} has no columns, aborting...")
        return

    new_columns = [column[0] for column in columns_info]
    datatypes = ['%s' for _ in new_columns]

    data = [(None,) * len(new_columns) for _ in range(num_rows)]

    sql = f"INSERT INTO {tbl_name} ({', '.join(new_columns)}) VALUES ({', '.join(datatypes)})"

    try:
        cursor.executemany(sql, data)
        database.commit()
        print(f"Successfully added {num_rows} rows to {tbl_name}.")
    except Exception as e:
        print(f"{indent}Error inserting data: {e}")

def remove_atribute_from_tbl(tbl_name: str, removed_columns: list[str]) -> None:
    print(f"{indent}Attempting to remove atribute(s)...")

    if not tbl_name or not removed_columns:
        print(f"{indent}Missing required inputs, aborting...")
        return

    for column in removed_columns:
        try:
            cursor.execute(f"ALTER TABLE {tbl_name} DROP COLUMN {column};")
        except Exception as e:
            print(f"{indent}Error removing column {column}: {e}")
            return

    database.commit()
    print(f"{indent}Successfully removed {', '.join(removed_columns)} from {tbl_name}")

def remove_record_from_tbl(tbl_name: str, row_ids: str) -> None:
    print(f"\n{indent}Attempting to remove records...")

    if not tbl_name or not row_ids:
        print(f"{indent}Missing table name or row IDs, aborting...")
        return

    if not check_tbl(tbl_name):
        print(f"Table {tbl_name} doesn't exist, aborting...")
        return

    try:
        id_list = [int(row_id.strip()) for row_id in row_ids.split(',')]
    except ValueError:
        print(f"{indent}Invalid row ID format. Please provide a comma-separated list of integers.")
        return

    if not id_list:
        print(f"{indent}No valid row IDs provided, aborting...")
        return

    placeholders = ', '.join(['%s'] * len(id_list))
    sql = f"DELETE FROM {tbl_name} WHERE id IN ({placeholders})"

    try:
        cursor.execute(sql, id_list)
        database.commit()
        print(f"{indent}Successfully deleted rows")
    except Exception as e:
        print(f"{indent}Error deleting rows: {e}")

# data functions ---------------------------------------------------------------------------------------

def insert_data_into_tbl_records(tbl_name: str = '', row_ids: list[int] = [], row_data: list[list[str]] = []) -> None:

    print(f"\n{indent}Inserting data into rows...")

    if not tbl_name:
        print(f"{indent}No table name provided, aborting...")
        return

    if not check_tbl(tbl_name):
        print(f"{indent}Table {tbl_name} doesn't exist, aborting...")
        return

    if not row_ids:
        print(f"{indent}No row IDs provided, aborting...")
        return

    if not row_data:
        print(f"{indent}No row data provided, aborting...")
        return

    if len(row_data) != len(row_ids):
        print(f"{indent}Number of rows does not match the data, aborting...")
        return

    cursor.execute(
        f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = %s ORDER BY ORDINAL_POSITION;",
        (tbl_name,)
    )
    columns: list[str] = [column[0] for column in cursor.fetchall()]

    if "id" in columns:
        columns.remove("id")

    if any(len(row) != len(columns) for row in row_data):
        print(f"{indent}Row data does not match the number of columns, aborting...")
        return

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
        values.append(str(row_id))
        cursor.execute(sql, values)

    database.commit()
    print(f"{indent}Successfully updated data in rows")

def insert_data_into_attributes(tbl_name: str = '', attributes: list[str] = [''], data: list[list] = []) -> None:
    transposed_data = list(zip(*data))

    if not check_tbl(tbl_name):
        print(f"{indent}Table doesnt exist, aborting...")
        return None

    if not attributes or attributes == ['']:
        print(f"{indent}No attributes inputed, aborting...")
        return None

    cursor.execute(f"DELETE FROM {tbl_name}")
    database.commit()
    
    sql = f"INSERT INTO {tbl_name} ({', '.join(attributes)}) VALUES ({', '.join(['%s' for _ in attributes])})"

    cursor.executemany(sql, transposed_data)
    database.commit()
    print(f"{indent}Successfully inserted data into atributes")

    print(f"{indent}Fixing row ids...")
    reset_tbl_row_ids(tbl_name)
    print(f"{indent}Row ids fixed")

# print functions --------------------------------------------------------------------------------------

def print_all_tbl_names() -> None:
    tbl_names = get_tbl_names()
    tbl_id = len(tbl_names)
    
    if tbl_id == 0:
        print(f"{indent}No tables exist")
        return None
    
    for i, table in zip(range(tbl_id), tbl_names):

        dashes = ff.dash_number(f"{table} {i}")
        if dashes: print(f"""{indent}{str(table)} {'-' * (dashes)} {i}{indent}""")
        else: print(f"""{indent}{str(table)} {'-' * (dashes)}{i}{indent}""")

def print_tbl(tbl_name :str, printed_columns: str = '*', params: str = '') -> None:
    if not check_tbl(tbl_name):
        print(f"\n{indent}Table {tbl_name} doesn't exist, aborting...")
        return None

    chosen_params = f"SELECT {printed_columns} FROM {tbl_name};"
    if params != '':
        chosen_params += f" WHERE {params}"

    cursor.execute(chosen_params)
    data = cursor.fetchall()

    if params == '':
        cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tbl_name}' ORDER BY ORDINAL_POSITION;")
    else: cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = {tbl_name} {params} ORDER BY ORDINAL_POSITION;")
    
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

def print_all_tbls(printed_columns: str = '*', params: str = ''):
    tables = get_tbl_names()
    for table in tables:
        print_tbl(table, printed_columns, params)

# misc functions ---------------------------------------------------------------------------------------

def check_tbl(tbl_name :str = '') -> bool:
    # remove table if it exists to stop crash
    if tbl_name == '':
        return False
    cursor.execute(f"SHOW TABLES LIKE '{tbl_name}';")
    return cursor.fetchall() != []

def add_tbl_row_id_column(tbl_name :str = '') -> None:
    print(f"\n{indent}Adding id column...")

    if not check_tbl(tbl_name):
        print(f"{indent}{tbl_name} doesn't exist, aborting...")
        return None

    cursor.execute(f"DESCRIBE {tbl_name}")
    column_names :list= [column[0] for column in cursor.fetchall()]

    if "id" not in column_names:
        try:
            cursor.execute(f"ALTER TABLE {tbl_name} ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY;")
            database.commit()
            print(f"{indent}Added id column to the table {tbl_name}")
        except connection.Error as err: print(f"{indent}Error adding id column: {err}")

def reset_tbl_row_ids(tbl_name: str, id_column: str = "id") -> None:

    try:
        print(f"\n{indent}Fixing id column for {tbl_name}...")
        
        cursor.execute(f"CREATE TEMPORARY TABLE temp_ids AS SELECT ROW_NUMBER() OVER (ORDER BY {id_column}) AS {id_column}, {id_column} AS old_id FROM {tbl_name};")
        
        cursor.execute(f"UPDATE {tbl_name} AS original JOIN temp_ids AS temp ON original.{id_column} = temp.old_id SET original.{id_column} = temp.{id_column};")

        cursor.execute("DROP TEMPORARY TABLE temp_ids;")

        database.commit()
        print(f"Successfully reset row IDs for {tbl_name}.")
    except Exception as e: print(f"{indent}Error while fixing row IDs: {str(e)}")

def get_tbl_names() -> list[str]:
    cursor.execute("SHOW TABLES")
    tbl_names = [tbl_name[0] for tbl_name in cursor.fetchall()]
    return tbl_names

# ------------------------------------------------------------------------------------------------------

print(f"\n{indent}Succesfully imported database functions")