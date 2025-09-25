from MonetDBtesting import tpymonetdb as pymonetdb
import os
from concurrent.futures import ThreadPoolExecutor

executor = ThreadPoolExecutor
if os.name == 'posix' and os.uname().sysname == 'Linux':
    nr_clients = 32
    nr_queries = 1000
else:
    nr_clients = 16
    nr_queries = 500

h = os.getenv('MAPIHOST')
p = int(os.getenv('MAPIPORT'))
db = os.getenv('TSTDB')


def client(_):
    for x in range(0, nr_queries):
        conn = pymonetdb.connect(hostname=h, port=p, database=db,
                                 autocommit=True)
        cursor = conn.cursor()
        cursor.execute("select 1;")
        cursor.fetchall()
        conn.close()


with executor(nr_clients) as pool:
    pool.map(client, range(nr_clients))
