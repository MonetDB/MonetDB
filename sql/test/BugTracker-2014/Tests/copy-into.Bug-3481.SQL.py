from MonetDBtesting import tpymonetdb as pymonetdb
import os, sys

def connect():
    return pymonetdb.connect(database = os.getenv('TSTDB'),
                             hostname = '127.0.0.1',
                             port = int(os.getenv('MAPIPORT')),
                             username = 'monetdb',
                             password = 'monetdb',
                             autocommit = True)

def query(conn, sql, result=False):
    cur = conn.cursor()
    cur.execute(sql)
    r = False
    if (result):
        r = cur.fetchall()
    cur.close()
    return r


# def open(conn):
#     conn.connect(database=os.getenv('TSTDB'),username="monetdb",password="monetdb",language="sql",hostname=os.getenv('MAPIHOST'),port=int(os.getenv('MAPIPORT')))

create1 = """-- some comment
START TRANSACTION;
CREATE TABLE a(i integer);
COPY 1 RECORDS INTO a FROM STDIN USING DELIMITERS ',',E'\\n','"';
42
COMMIT;
"""

create2 = """-- some comment
START TRANSACTION;
CREATE TABLE b(i integer);
COPY 1 RECORDS INTO b FROM STDIN USING DELIMITERS ',',E'\\n','"';
42
CREATE TABLE c(i integer);
COPY 2 RECORDS INTO c FROM STDIN USING DELIMITERS ',',E'\\n','"';
42
84
COMMIT;
"""

cn = connect()
query(cn, create1);
cn.close()

cn = connect()
query(cn, create2);
cn.close()


# this should show a-c, but only shows a
cn = connect()

if query(cn, 'SELECT name FROM tables WHERE system=0', True) != [('a', ),('b', ),('c', )]:
    sys.stderr.write("Expected [('a', ),('b', ),('c', )]")
query(cn, 'DROP TABLE a')

cn.close()
