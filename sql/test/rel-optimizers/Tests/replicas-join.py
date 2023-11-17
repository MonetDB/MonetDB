import os, sys, tempfile, pymonetdb

try:
    from MonetDBtesting import process
except ImportError:
    import process

with tempfile.TemporaryDirectory() as farm_dir:
    os.mkdir(os.path.join(farm_dir, 'master'))
    os.mkdir(os.path.join(farm_dir, 'node2'))
    os.mkdir(os.path.join(farm_dir, 'node3'))

    with (
            process.server(mapiport='0', dbname='master',
                           dbfarm=os.path.join(farm_dir, 'master'),
                           stdin=process.PIPE, stdout=process.PIPE,
                           stderr=process.PIPE) as prc_1,
            process.server(mapiport='0', dbname='node2',
                           dbfarm=os.path.join(farm_dir, 'node2'),
                           stdin=process.PIPE, stdout=process.PIPE,
                           stderr=process.PIPE) as prc_2,
            process.server(mapiport='0', dbname='node3',
                           dbfarm=os.path.join(farm_dir, 'node3'),
                           stdin=process.PIPE, stdout=process.PIPE,
                           stderr=process.PIPE) as prc_3
    ):
        # create foo_p2 and members_n2 in node2
        conn2 = pymonetdb.connect(database='node2', port=prc_2.dbport, autocommit=True)
        cur2 = conn2.cursor()

        cur2.execute("create table foo_p2 (n int, m text)")
        if cur2.execute("insert into foo_p2 values (1, 'hello'), (2, 'world')") != 2:
            sys.stderr.write("2 rows inserted expected\n")

        cur2.execute("create table bar_r2 (n int, m text)")
        if cur2.execute("insert into bar_r2 values (10, 'alice'), (20, 'bob'), (30, 'tom')") != 3:
            sys.stderr.write("3 rows inserted expected\n")

        cur2.execute("create table buz_rmt (l int)")
        if cur2.execute("insert into buz_rmt values (42), (43)") != 2:
            sys.stderr.write("2 rows inserted expected\n")

        cur2.close()
        conn2.close()

        # create foo_p2 and members_n2 in node2
        conn3 = pymonetdb.connect(database='node3', port=prc_3.dbport, autocommit=True)
        cur3 = conn3.cursor()

        cur3.execute("create table foo_p3 (n int, m text)")
        if cur3.execute("insert into foo_p3 values (1, 'hello'), (2, 'world')") != 2:
            sys.stderr.write("2 row inserted expected\n")

        cur3.execute("create table bar_r3 (n int, m text)")
        if cur3.execute("insert into bar_r3 values (10, 'alice'), (20, 'bob'), (30, 'tom')") != 3:
            sys.stderr.write("3 rows inserted expected\n")

        cur3.close()
        conn3.close()

        # create foo_rpl and bar_rpl on master
        conn1 = pymonetdb.connect(database='master', port=prc_1.dbport, autocommit=True)
        cur1 = conn1.cursor()

        cur1.execute("create table foo_local (n int, m text)")
        if cur1.execute("insert into foo_local values (1, 'hello'), (2, 'world')") != 2:
            sys.stderr.write("2 rows inserted expected\n")
        cur1.execute("create remote table foo_p2 (n int, m text) on 'mapi:monetdb://localhost:"+str(prc_2.dbport)+"/node2'")
        cur1.execute("create remote table foo_p3 (n int, m text) on 'mapi:monetdb://localhost:"+str(prc_3.dbport)+"/node3'")
        cur1.execute("create replica table foo_rpl (n int, m text)")
        cur1.execute("alter table foo_rpl add table foo_local")
        cur1.execute("alter table foo_rpl add table foo_p2")
        cur1.execute("alter table foo_rpl add table foo_p3")

        cur1.execute("create table bar_local (n int, m text)")
        if cur1.execute("insert into bar_local values (10, 'alice'), (20, 'bob'), (30, 'tom')") != 3:
            sys.stderr.write("3 rows inserted expected\n")
        cur1.execute("create remote table bar_r2 (n int, m text) on 'mapi:monetdb://localhost:"+str(prc_2.dbport)+"/node2'")
        cur1.execute("create remote table bar_r3 (n int, m text) on 'mapi:monetdb://localhost:"+str(prc_3.dbport)+"/node3'")
        cur1.execute("create replica table bar_rpl (n int, m text)")
        cur1.execute("alter table bar_rpl add table bar_local")
        cur1.execute("alter table bar_rpl add table bar_r2")
        cur1.execute("alter table bar_rpl add table bar_r3")

        cur1.execute("create remote table buz_rmt (l int) on 'mapi:monetdb://localhost:"+str(prc_2.dbport)+"/node2'")

        exp_no_rows = 6
        p = cur1.execute("select * from foo_rpl, bar_rpl")
        if p != exp_no_rows:
            sys.stderr.write(f'Expecting {exp_no_rows}: we got {p}\n')

        exp_rows = [
            (1, 'hello', 10, 'alice'),
            (1, 'hello', 20, 'bob'),
            (1, 'hello', 30, 'tom'),
            (2, 'world', 10, 'alice'),
            (2, 'world', 20, 'bob'),
            (2, 'world', 30, 'tom')
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

        exp_no_rows = 12
        p = cur1.execute("select * from foo_rpl, bar_rpl, buz_rmt")
        if p != exp_no_rows:
            sys.stderr.write(f'Expecting {exp_no_rows}: we got {p}\n')

        exp_rows = [
            (1, 'hello', 10, 'alice', 42),
            (1, 'hello', 10, 'alice', 43),
            (1, 'hello', 20, 'bob', 42),
            (1, 'hello', 20, 'bob', 43),
            (1, 'hello', 30, 'tom', 42),
            (1, 'hello', 30, 'tom', 43),
            (2, 'world', 10, 'alice', 42),
            (2, 'world', 10, 'alice', 43),
            (2, 'world', 20, 'bob', 42),
            (2, 'world', 20, 'bob', 43),
            (2, 'world', 30, 'tom', 42),
            (2, 'world', 30, 'tom', 43)
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

