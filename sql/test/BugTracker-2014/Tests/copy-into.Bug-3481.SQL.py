import os
from pymonetdb import mapi

def open(conn):
    conn.connect(database=os.getenv('TSTDB'),username="monetdb",password="monetdb",language="sql",hostname=os.getenv('MAPIHOST'),port=int(os.getenv('MAPIPORT')))

works = """-- some comment
START TRANSACTION;
CREATE TABLE a(i integer);
COPY 1 RECORDS INTO a FROM STDIN USING DELIMITERS ',','\\n','"';
42
COMMIT;
"""

doesnotwork = """-- some comment
START TRANSACTION;
CREATE TABLE b(i integer);
COPY 1 RECORDS INTO b FROM STDIN USING DELIMITERS ',','\\n','"';
42
CREATE TABLE c(i integer);
COPY 2 RECORDS INTO c FROM STDIN USING DELIMITERS ',','\\n','"';
42
84
COMMIT;
"""

cn = mapi.Connection()

open(cn)
print(cn.cmd('s'+works+';\n'))
cn.disconnect()

open(cn)
print(cn.cmd('s'+doesnotwork+';\n'))
cn.disconnect()

# this should show a-e, but only shows a
open(cn)
print(cn.cmd('sSELECT name FROM tables WHERE system=0;\n'))
print(cn.cmd('sDROP TABLE a;\n'))

cn.disconnect()
