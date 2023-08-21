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

        cur1.execute("create table s1 (i int)")
        if cur1.execute("insert into s1 values (23), (42)") != 2:
            sys.stderr.write("2 rows inserted expected")

        cur1.close()
        conn1.close()

        # node2 is the master
        prc2 = None
        with process.server(mapiport='0', dbname='node2',
                            dbfarm=os.path.join(farm_dir, 'node2'),
                            stdin=process.PIPE, stdout=process.PIPE,
                            stderr=process.PIPE) as prc2:
            conn2 = pymonetdb.connect(database='node2', port=prc2.dbport, autocommit=True)
            cur2 = conn2.cursor()

            # add s1 from node1 as remote
            cur2.execute("create remote table s1 (i int) on 'mapi:monetdb://localhost:"+str(prc1.dbport)+"/node1';")

            # create a local s2 identical with the remote s1
            cur2.execute("create table s2 (i int)")
            cur2.execute("insert into s2 values (23), (42)")

            # create the replica table with s1 (remote) and s2 (local)
            cur2.execute("create replica table repS(i int)")
            cur2.execute("alter table repS add table s1")
            cur2.execute("alter table repS add table s2")

            # create a new user and give him select perm on the repS
            cur2.execute("create user tom with password '123tom' name 'Tom' schema sys")
            cur2.execute("grant select on reps to tom")

            # close
            cur2.close()
            conn2.close()

            # connect as the newly created user
            conn3 = pymonetdb.connect(database='node2', port=prc2.dbport,
                                      username='tom', password='123tom',
                                      autocommit=True)
            cur3 = conn3.cursor()

            # even though the user can only select the repS table they
            # should NOT be blocked from selecting the data which are in
            # s2, even though they do not have select perms on s2
            cur3.execute("select * from repS")

            res = cur3.fetchall()
            col = [r[0] for r in res]
            if 23 not in col and 42 not in col:
                sys.stderr.write(f"quering replica result {res} is wrong")

            # close
            cur3.close()
            conn3.close()

            prc2.communicate()

        # cleanup: shutdown the monetdb servers and remove tempdir
        prc1.communicate()
