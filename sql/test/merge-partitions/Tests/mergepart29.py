from __future__ import print_function

import os
import socket
import sys
import tempfile
import shutil
import pymonetdb

try:
    from MonetDBtesting import process
except ImportError:
    import process


def freeport():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('', 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


farm_dir = tempfile.mkdtemp()

os.mkdir(os.path.join(farm_dir, 'db1'))
node1_port = freeport()
node1_proc = process.server(mapiport=node1_port, dbname='db1', dbfarm=os.path.join(farm_dir, 'db1'), stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE)
node1_conn = pymonetdb.connect(database='db1', port=node1_port, autocommit=True)
node1_cur = node1_conn.cursor()

os.mkdir(os.path.join(farm_dir, 'db2'))
node2_port = freeport()
node2_proc = process.server(mapiport=node2_port, dbname='db2', dbfarm=os.path.join(farm_dir, 'db2'), stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE)
node2_conn = pymonetdb.connect(database='db2', port=node2_port, autocommit=True)
node2_cur = node2_conn.cursor()

node2_cur.execute('CREATE TABLE "tb2" ("col1" int, "col2" int);')
node2_cur.execute('INSERT INTO "tb2" VALUES (1, 1), (2, 2), (3, 3);')

node1_cur.execute('CREATE MERGE TABLE "tb1" ("col1" int, "col2" int) PARTITION BY RANGE ON ("col1");')
node1_cur.execute('CREATE REMOTE TABLE "tb2" ("col1" int, "col2" int) ON \'mapi:monetdb://localhost:'+str(node2_port)+'/db2\';')
try:
    node1_cur.execute('ALTER TABLE "tb1" ADD TABLE "tb2" AS PARTITION FROM 0 TO 1;')  # error
except Exception as ex:
    sys.stderr.write(ex.__str__())
node1_cur.execute('ALTER TABLE "tb1" ADD TABLE "tb2" AS PARTITION FROM 0 TO 100;')
try:
    node1_cur.execute('INSERT INTO "tb1" VALUES (4, 4)')  # TODO, inserts on remote tables
except Exception as ex:
    sys.stderr.write(ex.__str__())
node1_cur.execute('SELECT "col1", "col2" FROM "tb1";')
output = node1_cur.fetchall()

out2, err2 = node2_proc.communicate()
sys.stdout.write(out2)
sys.stderr.write(err2)
print('\n')
out1, err1 = node1_proc.communicate()
sys.stdout.write(out1)
sys.stderr.write(err1)
print(output)

shutil.rmtree(farm_dir)
