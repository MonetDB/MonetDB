from __future__ import print_function

import os
import socket
import sys
import tempfile
import threading
import shutil

import pymonetdb

try:
    from MonetDBtesting import process
except ImportError:
    import process


# Find a free network port
def freeport():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('', 0))
    port = sock.getsockname()[1]
    sock.close()
    return port

def query(conn, sql):
    print(sql)
    cur = conn.cursor()
    cur.execute(sql)
    r = cur.fetchall()
    cur.close()
    return r

farm_dir = tempfile.mkdtemp()

# node1 is the worker
prt1 = freeport()
prc1 = process.server(mapiport=prt1, dbname='node1', dbfarm=os.path.join(farm_dir, 'node1'), stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE)
conn1 = pymonetdb.connect(database='node1', port=prt1, autocommit=True)

q = "create table t1 (i int, v varchar(10))"
print(q); conn1.execute(q)
q = "insert into t1 values (48, 'foo'), (29, 'bar'), (63, 'abc')"
print(q); conn1.execute(q)

# node2 is the master
prt2 = freeport()
prc2 = process.server(mapiport=prt2, dbname='node2', dbfarm=os.path.join(farm_dir, 'node2'), stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE)
conn2 = pymonetdb.connect(database='node2', port=prt2, autocommit=True)

q = "create remote table t1 (i int, v varchar(10)) on 'mapi:monetdb://localhost:"+str(prt1)+"/node1';"
print("#"+q); conn2.execute(q)

print(query(conn2, "select * from t1"))
print(query(conn2, "select * from t1 where i < 50"))
print(query(conn2, "select * from t1 where i > 50"))
print(query(conn2, "select * from t1 where i <> 50"))    # WRONG
print(query(conn2, "select * from t1 where v = 'foo'"))
print(query(conn2, "select * from t1 where v <> 'foo'")) # WRONG
print(query(conn2, "select * from t1 where v <> 'bla'")) # WRONG

# cleanup: shutdown the monetdb servers and remove tempdir
out, err = prc1.communicate()
sys.stdout.write(out)
sys.stderr.write(err)

out, err = prc2.communicate()
sys.stdout.write(out)
sys.stderr.write(err)

shutil.rmtree(farm_dir)
