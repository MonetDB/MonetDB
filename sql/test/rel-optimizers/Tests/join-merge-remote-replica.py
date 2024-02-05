import os, sys, tempfile, pymonetdb

try:
    from MonetDBtesting import process
except ImportError:
    import process

with tempfile.TemporaryDirectory() as farm_dir:
    os.mkdir(os.path.join(farm_dir, 'master'))
    os.mkdir(os.path.join(farm_dir, 'node2'))
    os.mkdir(os.path.join(farm_dir, 'node3'))

    with process.server(mapiport='0', dbname='master',
                        dbfarm=os.path.join(farm_dir, 'master'),
                        stdin=process.PIPE, stdout=process.PIPE,
                        stderr=process.PIPE) as prc_1, \
         process.server(mapiport='0', dbname='node2',
                        dbfarm=os.path.join(farm_dir, 'node2'),
                        stdin=process.PIPE, stdout=process.PIPE,
                        stderr=process.PIPE) as prc_2, \
         process.server(mapiport='0', dbname='node3',
                        dbfarm=os.path.join(farm_dir, 'node3'),
                        stdin=process.PIPE, stdout=process.PIPE,
                        stderr=process.PIPE) as prc_3:
        # create foo_p2 and members_n2 in node2
        conn2 = pymonetdb.connect(database='node2', port=prc_2.dbport, autocommit=True)
        cur2 = conn2.cursor()

        cur2.execute("create table foo_p2 (n int, m text)")
        if cur2.execute("insert into foo_p2 values (4, 'I'), (5, 'am'), (6, 'node2!')") != 3:
            sys.stderr.write("3 rows inserted expected\n")

        cur2.execute("create table members_n2 (n int, m text)")
        if cur2.execute("insert into members_n2 values (1, 'alice'), (2, 'bob')") != 2:
            sys.stderr.write("2 rows inserted expected\n")

        cur2.close()
        conn2.close()

        # create foo_p2 and members_n2 in node2
        conn3 = pymonetdb.connect(database='node3', port=prc_3.dbport, autocommit=True)
        cur3 = conn3.cursor()

        cur3.execute("create table foo_p3 (n int, m text)")
        if cur3.execute("insert into foo_p3 values (7, 'hi'), (8, 'from'), (9, 'node3!')") != 3:
            sys.stderr.write("3 rows inserted expected\n")

        cur3.execute("create table members_n3 (n int, m text)")
        if cur3.execute("insert into members_n3 values (1, 'alice'), (2, 'bob')") != 2:
            sys.stderr.write("2 rows inserted expected\n")

        cur3.close()
        conn3.close()

        # create foo_merge and member_rpl on master
        conn1 = pymonetdb.connect(database='master', port=prc_1.dbport, autocommit=True)
        cur1 = conn1.cursor()

        cur1.execute("create table foo_p1 (n int, m text)")
        if cur1.execute("insert into foo_p1 values (1, 'hello'), (2, 'world'), (3, '!!')") != 3:
            sys.stderr.write("3 rows inserted expected\n")
        cur1.execute("create remote table foo_p2 (n int, m text) on 'mapi:monetdb://localhost:"+str(prc_2.dbport)+"/node2'")
        cur1.execute("create remote table foo_p3 (n int, m text) on 'mapi:monetdb://localhost:"+str(prc_3.dbport)+"/node3'")
        cur1.execute("create merge table foo_merge (n int, m text)")
        cur1.execute("alter table foo_merge add table foo_p1")
        cur1.execute("alter table foo_merge add table foo_p2")
        cur1.execute("alter table foo_merge add table foo_p3")

        cur1.execute("create table members_n1 (n int, m text)")
        if cur1.execute("insert into members_n1 values (1, 'alice'), (2, 'bob')") != 2:
            sys.stderr.write("2 rows inserted expected\n")
        cur1.execute("create remote table members_n2 (n int, m text) on 'mapi:monetdb://localhost:"+str(prc_2.dbport)+"/node2'")
        cur1.execute("create remote table members_n3 (n int, m text) on 'mapi:monetdb://localhost:"+str(prc_3.dbport)+"/node3'")
        cur1.execute("create replica table members_rpl (n int, m text)")
        cur1.execute("alter table members_rpl add table members_n1")
        cur1.execute("alter table members_rpl add table members_n2")
        cur1.execute("alter table members_rpl add table members_n3")

        exp_no_rows = 18
        p = cur1.execute("select * from foo_merge, members_rpl")
        if p != exp_no_rows:
            sys.stderr.write(f'Expecting {exp_no_rows}: we got {p}\n')

        exp_rows = [
            (1, 'hello', 1, 'alice'),
            (1, 'hello', 2, 'bob'),
            (2, 'world', 1, 'alice'),
            (2, 'world', 2, 'bob'),
            (3, '!!', 1, 'alice'),
            (3, '!!', 2, 'bob'),
            (4, 'I', 1, 'alice'),
            (4, 'I', 2, 'bob'),
            (5, 'am', 1, 'alice'),
            (5, 'am', 2, 'bob'),
            (6, 'node2!', 1, 'alice'),
            (6, 'node2!', 2, 'bob'),
            (7, 'hi', 1, 'alice'),
            (7, 'hi', 2, 'bob'),
            (8, 'from', 1, 'alice'),
            (8, 'from', 2, 'bob'),
            (9, 'node3!', 1, 'alice'),
            (9, 'node3!', 2, 'bob')
        ]
        res = cur1.fetchall()
        for r in res:
            if r not in exp_rows:
                sys.stderr.write(f'Result row {r} is not expected\n')
            else:
                exp_rows.remove(r)

        if len(exp_rows) != 0:
            sys.stderr.write(f'Some expected results where not detected:\n')
            for r in exp_rows:
                sys.stderr.write(str(r)+'\n')

        cur1.close()
        conn1.close()

        prc_1.communicate()
        prc_2.communicate()
        prc_3.communicate()
