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
                           stderr=process.PIPE) as prc_2
    ):
        # create buz_rmt in node2
        conn2 = pymonetdb.connect(database='node2', port=prc_2.dbport, autocommit=True)
        cur2 = conn2.cursor()

        cur2.execute("create table buz_rmt (l int)")
        if cur2.execute("insert into buz_rmt values (10), (20), (30) ") != 3:
            sys.stderr.write("3 rows inserted expected\n")

        cur2.close()
        conn2.close()

        # create foo_rpl_loc and remote buz_rmt on master
        conn1 = pymonetdb.connect(database='master', port=prc_1.dbport, autocommit=True)
        cur1 = conn1.cursor()

        cur1.execute("create table foo_local (n int, m text)")
        if cur1.execute("insert into foo_local values (1, 'hello'), (2, 'world'), (3, '!!')") != 3:
            sys.stderr.write("3 rows inserted expected\n")

        cur1.execute("create remote table buz_rmt (l int) on 'mapi:monetdb://localhost:"+str(prc_2.dbport)+"/node2'")

        exp_no_rows = 9
        p = cur1.execute("select * from foo_local, buz_rmt")
        if p != exp_no_rows:
            sys.stderr.write(f'Expecting {exp_no_rows}: we got {p}\n')

        exp_rows = [
            (1, 'hello', 10),
            (1, 'hello', 20),
            (1, 'hello', 30),
            (2, 'world', 10),
            (2, 'world', 20),
            (2, 'world', 30),
            (3, '!!', 10),
            (3, '!!', 20),
            (3, '!!', 30)
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
