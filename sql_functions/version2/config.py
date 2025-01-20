from mysql.connector import connect

# formatting details
indent_length = 22
char_length = 30

server = {
    "host": "localhost",
    "user": "root",
    "password": "Sidemen1",
    "database": "test_db",
}
database = connect(**server)
cursor = database.cursor()

def get_cursor():
    return cursor

def get_database():
    return database

def get_char_length():
    return char_length

def get_indent_length():
    return indent_length

def get_indent():
    return " " * indent_length

def get_bar():
    return "-" * (indent_length * 2 + char_length)

def get_api_key():
    return f"&key=KEY_HERE"  #google api key

def get_url():
    return f"https://www.googleapis.com/books/v1/volumes"

#server details


