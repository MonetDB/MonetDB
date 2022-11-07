import os, sys, tempfile, pymonetdb

try:
    from MonetDBtesting import process
except ImportError:
    import process

with tempfile.TemporaryDirectory() as farm_dir:
    os.mkdir(os.path.join(farm_dir, 'node1'))

    with process.server(
                    dbname='node1',
                    dbfarm=os.path.join(farm_dir, 'node1'),
                    stdin=process.PIPE,
                    stdout=process.PIPE,
                    stderr=process.PIPE,
                    mapiport='0') as node1_proc:
            node1_conn = pymonetdb.connect(database='node1', port=node1_proc.dbport, autocommit=True)
            node1_cur = node1_conn.cursor()

            node1_cur.execute("CREATE unlogged TABLE foo (i INT)")
            res = node1_cur.execute("INSERT INTO foo VALUES (10), (20)")
            if res != 2:
                sys.stderr.write(f"2 rows inserted expected, {res} received")
            res = node1_cur.execute("UPDATE foo set i = i + 20 WHERE i = 10")
            if res != 1:
                sys.stderr.write(f"1 row updated expected, {res} received")
            node1_cur.execute("SELECT i FROM foo ORDER BY i")
            res = node1_cur.fetchall()
            if res != [(20,), (30,)]:
                sys.stderr.write(f"[(20), (30)] expected, {res} received")
            node1_cur.close()
            node1_conn.close()

    with process.server(
                    dbname='node1',
                    dbfarm=os.path.join(farm_dir, 'node1'),
                    stdin=process.PIPE,
                    stdout=process.PIPE,
                    stderr=process.PIPE,
                    mapiport='0') as node1_proc:
            node1_conn = pymonetdb.connect(database='node1', port=node1_proc.dbport, autocommit=True)
            node1_cur = node1_conn.cursor()

            node1_cur.execute("SELECT i FROM foo ORDER BY i")
            res = node1_cur.fetchall()
            if res != []:
                sys.stderr.write(f"[] expected, {res} received")
            node1_cur.close()
            node1_conn.close()

with tempfile.TemporaryDirectory() as farm_dir:
    os.mkdir(os.path.join(farm_dir, 'node1'))

    with process.server(
                    dbname='node1',
                    dbfarm=os.path.join(farm_dir, 'node1'),
                    stdin=process.PIPE,
                    stdout=process.PIPE,
                    stderr=process.PIPE,
                    mapiport='0') as node1_proc:
            node1_conn = pymonetdb.connect(database='node1', port=node1_proc.dbport, autocommit=True)
            node1_cur = node1_conn.cursor()

            node1_cur.execute("CREATE unlogged TABLE foo (i) AS VALUES (10), (20)")
            res = node1_cur.execute("INSERT INTO foo VALUES (30), (40)")
            if res != 2:
                sys.stderr.write(f"2 rows inserted expected, {res} received")
            res = node1_cur.execute("UPDATE foo set i = 50 WHERE i = 10")
            if res != 1:
                sys.stderr.write(f"1 rows updated expected, {res} received")
            node1_cur.execute("SELECT i FROM foo ORDER BY i")
            res = node1_cur.fetchall()
            if res != [(20,), (30,), (40,), (50,)]:
                sys.stderr.write(f"[(20), (30), (40), (50)] expected, {res} received")
            node1_cur.close()
            node1_conn.close()

    with process.server(
                    dbname='node1',
                    dbfarm=os.path.join(farm_dir, 'node1'),
                    stdin=process.PIPE,
                    stdout=process.PIPE,
                    stderr=process.PIPE,
                    mapiport='0') as node1_proc:
            node1_conn = pymonetdb.connect(database='node1', port=node1_proc.dbport, autocommit=True)
            node1_cur = node1_conn.cursor()

            node1_cur.execute("SELECT i FROM foo ORDER BY i")
            res = node1_cur.fetchall()
            if res not in ([(10,), (20,)], [(20,), (50,)]):
                sys.stderr.write(f"[(10,), (20,)] or [(20,), (50,)] expected, {res} received")
            node1_cur.close()
            node1_conn.close()
