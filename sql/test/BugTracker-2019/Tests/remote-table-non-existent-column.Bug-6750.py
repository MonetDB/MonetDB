import os, socket, sys, tempfile, pymonetdb

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

with tempfile.TemporaryDirectory() as farm_dir:
    prt1 = freeport()
    os.makedirs(os.path.join(farm_dir, 'node1'))
    with process.server(mapiport=prt1, dbname='node1', dbfarm=os.path.join(farm_dir, 'node1'), stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as prc1:
        conn1 = pymonetdb.connect(database='node1', port=prt1, autocommit=True)
        cur1 = conn1.cursor()
        cur1.execute("start transaction;")
        cur1.execute("create table tab1 (col1 clob);")
        if cur1.execute("insert into tab1 values ('a');") != 1:
            sys.stderr.write("1 row inserted expected")
        cur1.execute("create table tab2 (col1 tinyint);")
        if cur1.execute("insert into tab2 values (1);") != 1:
            sys.stderr.write("1 row inserted expected")
        cur1.execute("commit;")

        prt2 = freeport()
        os.makedirs(os.path.join(farm_dir, 'node2'))
        with process.server(mapiport=prt2, dbname='node2',
                            dbfarm=os.path.join(farm_dir, 'node2'),
                            stdin=process.PIPE, stdout=process.PIPE,
                            stderr=process.PIPE) as prc2:
            conn2 = pymonetdb.connect(database='node2', port=prt2, autocommit=True)
            cur2 = conn2.cursor()
            cur2.execute("create remote table tab1 (col1 clob, col2 int) on 'mapi:monetdb://localhost:"+str(prt1)+"/node1';")
            cur2.execute("create remote table tab2 (col1 double) on 'mapi:monetdb://localhost:"+str(prt1)+"/node1';")
            try:
                cur2.execute("select col2 from tab1;")  # col2 doesn't exist
                sys.stderr.write('Exception expected')
            except pymonetdb.OperationalError as e:
                if 'Identifier tab1.col2 doesn\'t exist' not in str(e):
                    sys.stderr.write(str(e))
            try:
                cur2.execute("select col1 from tab2;")  # col1 is not a floating point column
                sys.stderr.write('Exception expected')
            except pymonetdb.OperationalError as e:
                if 'Parameter 1 has wrong SQL type, expected double, but got tinyint instead' not in str(e):
                    sys.stderr.write(str(e))
            cur2.execute("drop table tab1;")
            cur2.execute("drop table tab2;")

            # Remote tables referencing merge tables in a loop
            cur1.execute("create merge table m1 (col1 clob);")
            cur2.execute("create merge table m2 (col1 clob);")
            cur1.execute("create remote table m2 (col1 clob) on 'mapi:monetdb://localhost:"+str(prt2)+"/node2';")
            cur2.execute("create remote table m1 (col1 clob) on 'mapi:monetdb://localhost:"+str(prt1)+"/node1';")
            cur1.execute("alter table m1 add table m2;")
            cur2.execute("alter table m2 add table m1;")
            try:
                cur2.execute("select * from m2;")  # Infinite loop while resolving the children of m2
                sys.stderr.write('Exception expected')
            except pymonetdb.OperationalError as e:
                if 'Merge tables not supported under remote connections' not in str(e):
                    sys.stderr.write(str(e))

            cur1.close()
            conn1.close()
            cur2.close()
            conn2.close()
            prc2.communicate()
        prc1.communicate()
