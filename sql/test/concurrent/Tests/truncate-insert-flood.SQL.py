from concurrent.futures import ThreadPoolExecutor
import os, pymonetdb

nr_clients = 16

db = os.getenv("TSTDB")
port = os.getenv("MAPIPORT")

def client(id):
    conn = pymonetdb.connect(
                    database=db,
                    port=port,
                    autocommit=True)
    cursor = conn.cursor()

    init = f'''
    drop table if exists foo_{id};
    create table foo_{id} (c1, c2, c3, c4, c5) AS VALUES
    (10, 20, 30, 40, 50),
    (11, 21, 31, 41, 51),
    (12, 22, 32, 42, 52);
    set optimizer = 'minimal_fast';
    '''
    cursor.execute(init)

    truncate_and_insert_queries = f"""
    truncate foo_{id};
    insert into foo_{id} VALUES
    (10, 20, 30, 40, 50),
    (11, 21, 31, 41, 51),
    (12, 22, 32, 42, 52);
    """
    nr_queries = 1600
    for x in range(0, nr_queries):
        cursor.execute(truncate_and_insert_queries)

with ThreadPoolExecutor(nr_clients) as pool:
    pool.map(client, range(nr_clients))
