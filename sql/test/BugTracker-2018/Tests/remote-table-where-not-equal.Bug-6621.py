import os, sys, tempfile, pymonetdb

try:
    from MonetDBtesting import process
except ImportError:
    import process


with tempfile.TemporaryDirectory() as farm_dir:
    os.mkdir(os.path.join(farm_dir, 'node1'))
    os.mkdir(os.path.join(farm_dir, 'node2'))

    # node1 is the worker
    with process.server(mapiport='0', dbname='node1',
                        dbfarm=os.path.join(farm_dir, 'node1'),
                        stdin=process.PIPE, stdout=process.PIPE,
                        stderr=process.PIPE) as prc1:
        conn1 = pymonetdb.connect(database='node1', port=prc1.dbport, autocommit=True)
        cur1 = conn1.cursor()
        cur1.execute("create table t1 (i int, v varchar(10))")
        cur1.execute("insert into t1 values (48, 'foo'), (29, 'bar'), (63, 'abc')")

        cur1.close()
        conn1.close()

        # node2 is the master
        with process.server(mapiport='0', dbname='node2',
                            dbfarm=os.path.join(farm_dir, 'node2'),
                            stdin=process.PIPE, stdout=process.PIPE,
                            stderr=process.PIPE) as prc2:
            conn2 = pymonetdb.connect(database='node2', port=prc2.dbport, autocommit=True)
            cur2 = conn2.cursor()

            cur2.execute("create remote table t1 (i int, v varchar(10)) on 'mapi:monetdb://localhost:{}/node1';".format(prc1.dbport))

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
