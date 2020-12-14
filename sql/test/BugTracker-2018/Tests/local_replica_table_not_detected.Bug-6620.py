import os, socket, sys, tempfile, pymonetdb

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

with tempfile.TemporaryDirectory() as farm_dir:
    os.mkdir(os.path.join(farm_dir, 'node1'))
    os.mkdir(os.path.join(farm_dir, 'node2'))

    # node1 is the worker
    prt1 = freeport()
    with process.server(mapiport=prt1, dbname='node1',
                        dbfarm=os.path.join(farm_dir, 'node1'),
                        stdin=process.PIPE, stdout=process.PIPE,
                        stderr=process.PIPE) as prc1:
        conn1 = pymonetdb.connect(database='node1', port=prt1, autocommit=True)
        cur1 = conn1.cursor()
        cur1.execute("create table s1 (i int)")
        if cur1.execute("insert into s1 values (23), (42)") != 2:
            sys.stderr.write("2 rows inserted expected")
        cur1.execute("create table t1 (s varchar(10))")
        if cur1.execute("insert into t1 values ('abc'), ('efg')") != 2:
            sys.stderr.write("2 rows inserted expected")

        cur1.close()
        conn1.close()
        # node2 is the master
        prt2 = freeport()
        prc2 = None
        with process.server(mapiport=prt2, dbname='node2',
                            dbfarm=os.path.join(farm_dir, 'node2'),
                            stdin=process.PIPE, stdout=process.PIPE,
                            stderr=process.PIPE) as prc2:
            conn2 = pymonetdb.connect(database='node2', port=prt2, autocommit=True)
            cur2 = conn2.cursor()
            cur2.execute("create table s2 (i int)")
            if cur2.execute("insert into s2 values (23), (42)") != 2:
                sys.stderr.write("2 rows inserted expected")
            cur2.execute("create table t2 (s varchar(10))")
            if cur2.execute("insert into t2 values ('foo'), ('bar')") != 2:
                sys.stderr.write("2 rows inserted expected")

            cur2.execute("create remote table s1 (i int) on 'mapi:monetdb://localhost:"+str(prt1)+"/node1';")
            cur2.execute("create remote table t1 (s varchar(10)) on 'mapi:monetdb://localhost:"+str(prt1)+"/node1';")

            cur2.execute("create replica table repS(i int)")
            cur2.execute("alter table repS add table s1")
            cur2.execute("alter table repS add table s2")
            cur2.execute("create merge table mrgT (s varchar(10))")
            cur2.execute("alter table mrgT add table t1")
            cur2.execute("alter table mrgT add table t2")

            cur2.execute("plan select * from repS")
            for r in cur2.fetchall():
               if 'remote' in ''.join(r).lower():
                   sys.stderr.write('No REMOTE properties expected')

            cur2.execute("plan select * from repS, mrgT")
            for r in cur2.fetchall():
               if 'remote(sys.s1)' in ''.join(r).lower():
                   sys.stderr.write('remote(sys.s1) not expected')

            cur2.close()
            conn2.close()
            prc2.communicate()
        # cleanup: shutdown the monetdb servers and remove tempdir
        prc1.communicate()
