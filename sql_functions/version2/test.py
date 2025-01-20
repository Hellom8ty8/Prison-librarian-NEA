from google_books_api import api_functions as apif
import config

url = config.get_url()

queries = ""
queries += apif.add_query('Harry Potter', queries)
queries = apif.add_query('JK Rowling', queries)

url = apif.format_url(url, queries)
data = apif.send(url)
