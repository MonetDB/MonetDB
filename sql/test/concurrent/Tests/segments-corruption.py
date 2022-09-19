import pyodbc
import time
from multiprocessing import Pool
import random

CONN_STR = 'DRIVER={/libMonetODBC.so};HOST=localhost;PORT=50000;DATABASE=devdb;UID=monetdb;PWD=monetdb'
QUERIES = [
    "select * from test limit 1",
    "insert into test values (0, 0, 0, 0, 0)",
]
DURATION = 30
CLIENTS = 16

def run(_):
    commits = 0
    errors = 0
    conn = pyodbc.connect(CONN_STR)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute("set optimizer = 'default_fast'")

    begin = time.time()
    while time.time() - begin < DURATION:
        try:
            cursor.execute(QUERIES[random.randint(0, len(QUERIES)-1)])
        except:
            print('error')
            errors += 1
        commits += 1

    return commits, errors

conn = pyodbc.connect(CONN_STR)
conn.autocommit = True
cursor = conn.cursor()
cursor.execute('create table if not exists test (c1 int, c2 int, c3 int, c4 int, c5 int)')

pool = Pool(CLIENTS)
results = pool.map(run, [None for _ in range(CLIENTS)])
total = sum([x[0] for x in results])
errors = sum([x[1] for x in results])
print(f'{round(total / DURATION, 2)} tx/s')
print(errors)
