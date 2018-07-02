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


farm_dir = tempfile.mkdtemp()

node1_port = freeport()
node1_proc = process.server(mapiport=node1_port, dbname='node1', dbfarm=os.path.join(farm_dir, 'node1'), stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE)
node1_conn = pymonetdb.connect(database='node1', port=node1_port, autocommit=True)
node1_cur = node1_conn.cursor();

print("# node1: CREATE TABLE tbl (id INT, name TEXT)")
node1_cur.execute("CREATE TABLE tbl (id INT, name TEXT)")
print("# node1: INSERT INTO tbl VALUES (1, '1')")
node1_cur.execute("INSERT INTO tbl VALUES (1, '1')")
print("# node1: INSERT INTO tbl VALUES (2, '2')")
node1_cur.execute("INSERT INTO tbl VALUES (2, '2')")
print("# node1: INSERT INTO tbl (id) VALUES (3)")
node1_cur.execute("INSERT INTO tbl (id) VALUES (3)")
print("# node1: SELECT * FROM tbl")
node1_cur.execute("SELECT * FROM tbl")
print(node1_cur.fetchall())
print("# node1: SELECT * FROM tbl WHERE NAME IS NULL")
node1_cur.execute("SELECT * FROM tbl WHERE NAME IS NULL")
print(node1_cur.fetchall())
print("# node1: SELECT * FROM tbl")
node1_cur.execute("SELECT * FROM tbl")
print(node1_cur.fetchall())

node2_port = freeport()
node2_proc = process.server(mapiport=node2_port, dbname='node2', dbfarm=os.path.join(farm_dir, 'node2'), stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE)
node2_conn = pymonetdb.connect(database='node2', port=node2_port, autocommit=True)
node2_cur = node2_conn.cursor();

print("# node2: CREATE REMOTE TABLE tbl (id INT, name TEXT) on 'mapi:monetdb://localhost:{}/node1/sys/tbl'".format(node1_port))
node2_cur.execute("CREATE REMOTE TABLE tbl (id INT, name TEXT) on 'mapi:monetdb://localhost:{}/node1/sys/tbl'".format(node1_port))
print("# node2: SELECT * FROM tbl")
node2_cur.execute("SELECT * FROM tbl")
print(node2_cur.fetchall())
print("# node2: SELECT * FROM tbl WHERE NAME IS NULL")
node2_cur.execute("SELECT * FROM tbl WHERE NAME IS NULL")
print(node2_cur.fetchall())
print("# node2: SELECT * FROM tbl")
node2_cur.execute("SELECT * FROM tbl")
print(node2_cur.fetchall())

# cleanup: shutdown the monetdb servers and remove tempdir
out, err = node1_proc.communicate()
sys.stdout.write(out)
sys.stderr.write(err)

out, err = node1_proc.communicate()
sys.stdout.write(out)
sys.stderr.write(err)

shutil.rmtree(farm_dir)
