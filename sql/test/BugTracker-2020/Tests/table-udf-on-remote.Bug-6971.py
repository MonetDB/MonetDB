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

    node1_port = freeport()
    with process.server(mapiport=node1_port, dbname='node1',
                        dbfarm=os.path.join(farm_dir, 'node1'),
                        stdin=process.PIPE, stdout=process.PIPE,
                        stderr=process.PIPE) as node1_proc:
        node1_conn = pymonetdb.connect(database='node1', port=node1_port, autocommit=True)
        node1_cur = node1_conn.cursor()

        node1_cur.execute("create function mudf(sx float, sxx float, sxy float, sy float, syy float, n int) returns table(res float) begin return select 0.5; end")
        node1_cur.execute("create function mudf2(sx float, sxx float, sxy float, sy float, syy float, n int) returns table(res float, res2 float) begin return select 0.5, 0.6; end")
        node1_cur.execute("create table lala(sx float, sxx float, sxy float , sy float, syy float, n int)")
        if node1_cur.execute("insert into lala select 13,85,98,15,113,2") != 1:
            sys.stderr.write("1 row inserted expected")
        node1_cur.execute("select * from lala")
        if node1_cur.fetchall() != [(13.0, 85.0, 98.0, 15.0, 113.0, 2)]:
            sys.stderr.write("Just row (13.0, 85.0, 98.0, 15.0, 113.0, 2) expected")
        node1_cur.execute("select * from mudf((select * from lala))")
        if node1_cur.fetchall() != [(0.5,)]:
            sys.stderr.write("Just row (0.5,) expected")
        node1_cur.execute("select * from mudf2((select * from lala))")
        if node1_cur.fetchall() != [(0.5, 0.6)]:
            sys.stderr.write("Just row (0.5, 0.6) expected")

        node2_port = freeport()
        with process.server(mapiport=node2_port, dbname='node2',
                            dbfarm=os.path.join(farm_dir, 'node2'),
                            stdin=process.PIPE, stdout=process.PIPE,
                            stderr=process.PIPE) as node2_proc:
            node2_conn = pymonetdb.connect(database='node2', port=node2_port, autocommit=True)
            node2_cur = node2_conn.cursor()

            node2_cur.execute("create function mudf(sx float, sxx float, sxy float, sy float, syy float, n int) returns table(res float) begin return select 0.5; end")
            node2_cur.execute("create function mudf2(sx float, sxx float, sxy float, sy float, syy float, n int) returns table(res float, res2 float) begin return select 0.5, 0.6; end")
            node2_cur.execute("create remote table fofo(sx float, sxx float, sxy float, sy float, syy float, n int) on 'mapi:monetdb://localhost:{}/node1/sys/lala'".format(node1_port))
            node2_cur.execute("select * from fofo")
            if node2_cur.fetchall() != [(13.0, 85.0, 98.0, 15.0, 113.0, 2)]:
                sys.stderr.write("Just row (13.0, 85.0, 98.0, 15.0, 113.0, 2) expected")
            node2_cur.execute("select * from mudf((select * from fofo))")
            if node2_cur.fetchall() != [(0.5,)]:
                sys.stderr.write("Just row (0.5,) expected")
            try:
                node2_cur.execute("select * from mudf((select sx,sxx,sxy,sy,syy,'\"' from fofo))")
                sys.stderr.write('Exception expected')
            except pymonetdb.OperationalError as e:
                if 'to type int failed' not in str(e):
                    sys.stderr.write(str(e))
            node2_cur.execute("select * from mudf((select sx,sxx,sxy,sy,syy,1 as \"a\"\"a\" from fofo))")
            if node2_cur.fetchall() != [(0.5,)]:
                sys.stderr.write("Just row (0.5,) expected")
            node2_cur.execute("select * from mudf2((select * from fofo))")
            if node2_cur.fetchall() != [(0.5, 0.6)]:
                sys.stderr.write("Just row (0.5, 0.6) expected")

            # cleanup: shutdown the monetdb servers and remove tempdir
            node1_proc.communicate()
            node2_proc.communicate()
