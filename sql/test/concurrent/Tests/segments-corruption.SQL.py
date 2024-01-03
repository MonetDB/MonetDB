import os, random, pymonetdb
from concurrent.futures import ThreadPoolExecutor

init    =   '''
            drop table if exists foo;
            create table foo (c1 int, c2 int, c3 int, c4 int, c5 int);
            '''

queries =   [
            "select * from foo limit 1;",
            "insert into foo values (0, 0, 0, 0, 0);",
            ]

nr_clients  = 16
nr_queries  = 2000

h   = os.getenv('MAPIHOST')
p   = int(os.getenv('MAPIPORT'))
db  = os.getenv('TSTDB')

conn = pymonetdb.connect(hostname=h, port=p,database=db, autocommit=True)
cursor = conn.cursor()

try:
    cursor.execute(init)
except Exception as e:
            print(e)
            exit(1)

def client(_):
    conn = pymonetdb.connect(hostname=h, port=p,database=db, autocommit=True)
    cursor = conn.cursor()
    cursor.execute("set optimizer = 'default_fast';")

    for x in range(0, nr_queries):
        try:
            cursor.execute(queries[random.randint(0, len(queries)-1)])
        except Exception as e:
            print(e)
            exit(1)

with ThreadPoolExecutor(nr_clients) as pool:
    pool.map(client, range(nr_clients))
