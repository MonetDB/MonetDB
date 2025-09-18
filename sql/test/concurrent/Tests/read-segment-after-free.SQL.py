from MonetDBtesting import tpymonetdb as pymonetdb
import os
from concurrent.futures import ThreadPoolExecutor
from pymonetdb.exceptions import OperationalError

executor = ThreadPoolExecutor

init = '''
            drop table if exists foo;
            create table foo (c1, c2, c3, c4, c5) AS VALUES
            (10, 20, 30, 40, 50),
            (11, 21, 31, 41, 51),
            (12, 22, 32, 42, 52);
            '''

query = """
            truncate foo;
            insert into foo VALUES
            (10, 20, 30, 40, 50),
            (11, 21, 31, 41, 51),
            (12, 22, 32, 42, 52);
            """

h = os.getenv('MAPIHOST')
p = int(os.getenv('MAPIPORT'))
db = os.getenv('TSTDB')

nr_queries = 1000
nr_clients = 16

conn = pymonetdb.connect(hostname=h, port=p, database=db, autocommit=True)
cursor = conn.cursor()

try:
    cursor.execute(init)
except Exception as e:
    print(e)
    exit(1)


def client(_):
    conn = pymonetdb.connect(hostname=h, port=p, database=db, autocommit=True)
    cursor = conn.cursor()
    cursor.execute("set optimizer = 'minimal_fast';")

    for x in range(0, nr_queries):
        try:
            cursor.execute(query)
        except OperationalError:
            # concurrency conflicts are allowed
            pass


with executor(nr_clients) as pool:
    pool.map(client, range(nr_clients))
