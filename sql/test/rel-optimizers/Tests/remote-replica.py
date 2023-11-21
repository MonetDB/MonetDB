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
        # create foo_r2 and bar_r2 in node2
        conn2 = pymonetdb.connect(database='node2', port=prc_2.dbport, autocommit=True)
        cur2 = conn2.cursor()

        cur2.execute("create table foo_r2 (n int, m text)")
        if cur2.execute("insert into foo_r2 values (3, 'replica'), (4, 'data')") != 2:
            sys.stderr.write("2 rows inserted expected\n")

        cur2.execute("create table bar_r2 (n int, m text)")
        if cur2.execute("insert into bar_r2 values (10, 'alice'), (20, 'bob'), (30, 'tom')") != 3:
            sys.stderr.write("3 rows inserted expected\n")

        cur2.execute("create table buz_rmt (l int)")
        if cur2.execute("insert into buz_rmt values (42), (43)") != 2:
            sys.stderr.write("2 rows inserted expected\n")

        cur2.close()
        conn2.close()

        # create foo_r3 and bar_r3 in node3
        conn3 = pymonetdb.connect(database='node3', port=prc_3.dbport, autocommit=True)
        cur3 = conn3.cursor()

        cur3.execute("create table foo_r3 (n int, m text)")
        if cur3.execute("insert into foo_r3 values (3, 'replica'), (4, 'data')") != 2:
            sys.stderr.write("2 row inserted expected\n")

        cur3.execute("create table bar_r3 (n int, m text)")
        if cur3.execute("insert into bar_r3 values (10, 'alice'), (20, 'bob'), (30, 'tom')") != 3:
            sys.stderr.write("3 rows inserted expected\n")

        cur3.close()
        conn3.close()

        # create foo_rpl_rmt, bar_rpl_rmt, foo_rpl_rmt_node2 and bar_rpl_rmt_node3 on master
        conn1 = pymonetdb.connect(database='master', port=prc_1.dbport, autocommit=True)
        cur1 = conn1.cursor()

        cur1.execute("create remote table foo_r2 (n int, m text) on 'mapi:monetdb://localhost:"+str(prc_2.dbport)+"/node2'")
        cur1.execute("create remote table foo_r3 (n int, m text) on 'mapi:monetdb://localhost:"+str(prc_3.dbport)+"/node3'")
        cur1.execute("create replica table foo_rpl_rmt (n int, m text)")
        cur1.execute("alter table foo_rpl_rmt add table foo_r2")
        cur1.execute("alter table foo_rpl_rmt add table foo_r3")

        cur1.execute("create remote table bar_r2 (n int, m text) on 'mapi:monetdb://localhost:"+str(prc_2.dbport)+"/node2'")
        cur1.execute("create remote table bar_r3 (n int, m text) on 'mapi:monetdb://localhost:"+str(prc_3.dbport)+"/node3'")
        cur1.execute("create replica table bar_rpl_rmt (n int, m text)")
        cur1.execute("alter table bar_rpl_rmt add table bar_r2")
        cur1.execute("alter table bar_rpl_rmt add table bar_r3")

        cur1.execute("create replica table foo_rpl_rmt_node2 (n int, m text)")
        cur1.execute("alter table foo_rpl_rmt_node2 add table foo_r2")

        cur1.execute("create replica table bar_rpl_rmt_node3 (n int, m text)")
        cur1.execute("alter table bar_rpl_rmt_node3 add table bar_r3")

        cur1.execute("create remote table buz_rmt (l int) on 'mapi:monetdb://localhost:"+str(prc_2.dbport)+"/node2'")

        exp_no_rows = 6
        p = cur1.execute("select * from foo_rpl_rmt, bar_rpl_rmt")
        if p != exp_no_rows:
            sys.stderr.write(f'Expecting {exp_no_rows}: we got {p}\n')

        exp_rows = [
            (3, 'replica', 10, 'alice' ),
            (4, 'data', 10, 'alice' ),
            (3, 'replica', 20, 'bob' ),
            (4, 'data', 20, 'bob' ),
            (3, 'replica', 30, 'tom' ),
            (4, 'data', 30, 'tom' ),
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

        exp_no_rows = 6
        p = cur1.execute("select * from foo_rpl_rmt_node2, bar_rpl_rmt_node3")
        if p != exp_no_rows:
            sys.stderr.write(f'Expecting {exp_no_rows}: we got {p}\n')

        exp_rows = [
            (3, 'replica', 10, 'alice' ),
            (4, 'data', 10, 'alice' ),
            (3, 'replica', 20, 'bob' ),
            (4, 'data', 20, 'bob' ),
            (3, 'replica', 30, 'tom' ),
            (4, 'data', 30, 'tom' ),
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

        exp_no_rows = 4
        p = cur1.execute("select * from foo_rpl_rmt, buz_rmt")
        if p != exp_no_rows:
            sys.stderr.write(f'Expecting {exp_no_rows}: we got {p}\n')

        exp_rows = [
            (3, 'replica', 42),
            (4, 'data', 42),
            (3, 'replica', 43),
            (4, 'data', 43)
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

        exp_no_rows = 6
        p = cur1.execute("select * from bar_rpl_rmt_node3, buz_rmt")
        if p != exp_no_rows:
            sys.stderr.write(f'Expecting {exp_no_rows}: we got {p}\n')

        exp_rows = [
            (10, 'alice', 42),
            (20, 'bob', 42),
            (30, 'tom', 42),
            (10, 'alice', 43),
            (20, 'bob', 43),
            (30, 'tom', 43),
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
