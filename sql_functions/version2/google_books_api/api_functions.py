import config
import requests
from sql_functions import formatting_functions as ff

global key, indent

bar = config.get_bar()
indent = config.get_indent()
key = config.get_api_key()

print(f"\n{ff.TermCol.yellow}{indent}Attempting to import API functions...{ff.TermCol.ENDC}")

def add_query(query :str = '', querys :str = '') -> str:
    print(f"\n{ff.TermCol.cyan}{indent}Forming query...{ff.TermCol.ENDC}")

    if not query:
        print(f"\n{ff.TermCol.red}{indent}Error: No query provided\nERROR CODE 1{ff.TermCol.ENDC}")
        return querys

    query = query.replace(' ', '+')

    if querys: querys = f"{querys}&{query}"
    
    else: querys = query
    print(f"\n{ff.TermCol.green}{indent}Query added{ff.TermCol.ENDC}")

    return querys

def send(url :str='') -> int:
    print(f"\n{ff.TermCol.cyan}{indent}Sending request...{ff.TermCol.ENDC}")

    if not url or not key:
        print(f"\n{ff.TermCol.red}{indent}Error: No url/key provided\nERROR CODE 2{ff.TermCol.ENDC}")
        return 1

    url = url + key
    print(f"\n{ff.TermCol.cyan}{indent}url: {ff.TermCol.blue}{url}{ff.TermCol.ENDC}")
    try: response = requests.get(url)
    
    except Exception as e:
        print(f"\n{ff.TermCol.red}{indent}Error sending request: {e}{ff.TermCol.ENDC}")
        return 2

    print(f"\n{ff.TermCol.cyan}{indent}Response received...{ff.TermCol.ENDC}")

    if response.status_code != 200:
        print(f"{ff.TermCol.red}{indent}Request unsuccessfull: {response.status_code}, {response.text}{ff.TermCol.ENDC}")
        return None

    print(f"\n{ff.TermCol.green}{indent}Request successful{ff.TermCol.ENDC}")
    
    response = requests.get(url)

    if response.status_code != 200:
        print(f"\n{ff.TermCol.red}Error: {response.status_code}, {response.text}{ff.TermCol.ENDC}")
        return 3

    data = response.json()

    return data

def format_url(url :str = '', queries :str = '') -> str:
    if not queries: url = url + key
    else:
        url += f"?q={queries}{key}"
    return url

def print_details():
    pass #TODO print details of the book inputted into the function

print(f"\n{ff.TermCol.green}{indent}API functions imported{ff.TermCol.ENDC}")