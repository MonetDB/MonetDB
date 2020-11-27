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
        cur1.execute("create table t1 (i int, v varchar(10))")
        cur1.execute("insert into t1 values (48, 'foo'), (29, 'bar'), (63, 'abc')")

        cur1.close()
        conn1.close()

        # node2 is the master
        prt2 = freeport()
        with process.server(mapiport=prt2, dbname='node2',
                            dbfarm=os.path.join(farm_dir, 'node2'),
                            stdin=process.PIPE, stdout=process.PIPE,
                            stderr=process.PIPE) as prc2:
            conn2 = pymonetdb.connect(database='node2', port=prt2, autocommit=True)
            cur2 = conn2.cursor()

            cur2.execute("create remote table t1 (i int, v varchar(10)) on 'mapi:monetdb://localhost:{}/node1';".format(prt1))

            cur2.execute("select * from t1")
            if cur2.fetchall() != [(48, 'foo'), (29, 'bar'), (63, 'abc')]:
               sys.stderr.write("[(48, 'foo'), (29, 'bar'), (63, 'abc')] expected")
            cur2.execute("select * from t1 where i < 50")
            if cur2.fetchall() != [(48, 'foo'), (29, 'bar')]:
               sys.stderr.write("[(48, 'foo'), (29, 'bar')] expected")
            cur2.execute("select * from t1 where i > 50")
            if cur2.fetchall() != [(63, 'abc')]:
               sys.stderr.write("[(63, 'abc')] expected")
            cur2.execute("select * from t1 where i <> 50")
            if cur2.fetchall() != [(48, 'foo'), (29, 'bar'), (63, 'abc')]:
               sys.stderr.write("[(48, 'foo'), (29, 'bar'), (63, 'abc')] expected")
            cur2.execute("select * from t1 where v = 'foo'")
            if cur2.fetchall() != [(48, 'foo')]:
               sys.stderr.write("[(48, 'foo')] expected")
            cur2.execute("select * from t1 where v <> 'foo'")
            if cur2.fetchall() != [(29, 'bar'), (63, 'abc')]:
               sys.stderr.write("[(29, 'bar'), (63, 'abc')] expected")
            cur2.execute("select * from t1 where v <> 'bla'")
            if cur2.fetchall() != [(48, 'foo'), (29, 'bar'), (63, 'abc')]:
               sys.stderr.write("[(48, 'foo'), (29, 'bar'), (63, 'abc')] expected")

            cur2.close()
            conn2.close()

            # cleanup: shutdown the monetdb servers and remove tempdir
            prc1.communicate()
            prc2.communicate()
