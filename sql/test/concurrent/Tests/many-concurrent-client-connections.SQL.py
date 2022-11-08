import os, pymonetdb
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

if os.name == 'posix' and os.uname().sysname == 'Linux':
    executor = ProcessPoolExecutor
else:
    executor = ThreadPoolExecutor

h   = os.getenv('MAPIHOST')
p   = int(os.getenv('MAPIPORT'))
db  = os.getenv('TSTDB')

nr_clients = 32
def client(_):
    nr_queries = 1000
    for x in range(0, nr_queries):
        conn = pymonetdb.connect(hostname=h, port=p,database=db, autocommit=True)
        cursor = conn.cursor()
        cursor.execute("select 1;")
        cursor.fetchall()
        conn.close()


with executor(nr_clients) as pool:
    pool.map(client, range(nr_clients))
