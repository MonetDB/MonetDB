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

prt1 = freeport()
os.makedirs(os.path.join(farm_dir, 'node1'))
try:
    prc1 = process.server(mapiport=prt1, dbname='node1', dbfarm=os.path.join(farm_dir, 'node1'), stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE)
    try:
        conn1 = pymonetdb.connect(database='node1', port=prt1, autocommit=True)
        cur1 = conn1.cursor()
        cur1.execute("start transaction;")
        cur1.execute("create table tab1 (col1 clob);")
        cur1.execute("insert into tab1 values ('a');")
        cur1.execute("create table tab2 (col1 tinyint);")
        cur1.execute("insert into tab2 values (1);")
        cur1.execute("commit;")

        prt2 = freeport()
        os.makedirs(os.path.join(farm_dir, 'node2'))
        prc2 = process.server(mapiport=prt2, dbname='node2', dbfarm=os.path.join(farm_dir, 'node2'), stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE)
        try:
            conn2 = pymonetdb.connect(database='node2', port=prt2, autocommit=True)
            cur2 = conn2.cursor()
            cur2.execute("create remote table tab1 (col1 clob, col2 int) on 'mapi:monetdb://localhost:"+str(prt1)+"/node1';")
            cur2.execute("create remote table tab2 (col1 double) on 'mapi:monetdb://localhost:"+str(prt1)+"/node1';")
            try:
                cur2.execute("select col2 from tab1;")  # col2 doesn't exist
            except pymonetdb.OperationalError as e:
                print(e)
            try:
                cur2.execute("select col1 from tab2;")  # col1 is not a floating point column
            except pymonetdb.OperationalError as e:
                print(e)
            cur2.execute("drop table tab1;")
            cur2.execute("drop table tab2;")

            # Remote tables referencing merge tables in a loop
            cur1.execute("create merge table m1 (col1 clob);")
            cur2.execute("create merge table m2 (col1 clob);")
            cur1.execute("create remote table m2 (col1 clob) on 'mapi:monetdb://localhost:"+str(prt2)+"/node2';")
            cur2.execute("create remote table m1 (col1 clob) on 'mapi:monetdb://localhost:"+str(prt1)+"/node1';")
            cur1.execute("alter table m1 add table m2;")
            cur2.execute("alter table m2 add table m1;")
            cur2.execute("select * from m2;")

            cur1.close()
            conn1.close()
            cur2.close()
            conn2.close()
        finally:
            out, err = prc2.communicate()
            sys.stdout.write(out)
            sys.stderr.write(err)
    finally:
        out, err = prc1.communicate()
        sys.stdout.write(out)
        sys.stderr.write(err)
finally:
    shutil.rmtree(farm_dir)
