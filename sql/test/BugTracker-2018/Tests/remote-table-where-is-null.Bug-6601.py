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

        node1_cur.execute("CREATE TABLE tbl (id INT, name TEXT)")
        if node1_cur.execute("INSERT INTO tbl VALUES (1, '1'), (2, '2')") != 2:
            sys.stderr.write("2 rows inserted expected")
        if node1_cur.execute("INSERT INTO tbl (id) VALUES (3)") != 1:
            sys.stderr.write("1 row inserted expected")
        node1_cur.execute("SELECT * FROM tbl")
        if node1_cur.fetchall() != [(1, '1'), (2, '2'), (3, None)]:
            sys.stderr.write("[(1, '1'), (2, '2'), (3, None)] expected")
        node1_cur.execute("SELECT * FROM tbl WHERE NAME IS NULL")
        if node1_cur.fetchall() != [(3, None)]:
            sys.stderr.write("[(3, None)] expected")
        node1_cur.execute("SELECT * FROM tbl")
        if node1_cur.fetchall() != [(1, '1'), (2, '2'), (3, None)]:
            sys.stderr.write("[(1, '1'), (2, '2'), (3, None)] expected")

        node1_cur.close()
        node1_conn.close()

        node2_port = freeport()
        with process.server(mapiport=node2_port, dbname='node2',
                            dbfarm=os.path.join(farm_dir, 'node2'),
                            stdin=process.PIPE, stdout=process.PIPE,
                            stderr=process.PIPE) as node2_proc:
            node2_conn = pymonetdb.connect(database='node2', port=node2_port, autocommit=True)
            node2_cur = node2_conn.cursor()

            node2_cur.execute("CREATE REMOTE TABLE tbl (id INT, name TEXT) on 'mapi:monetdb://localhost:{}/node1/sys/tbl'".format(node1_port))
            node2_cur.execute("SELECT * FROM tbl")
            if node2_cur.fetchall() != [(1, '1'), (2, '2'), (3, None)]:
                sys.stderr.write("[(1, '1'), (2, '2'), (3, None)] expected")
            node2_cur.execute("SELECT * FROM tbl WHERE NAME IS NULL")
            if node2_cur.fetchall() != [(3, None)]:
                sys.stderr.write("[(3, None)] expected")
            node2_cur.execute("SELECT * FROM tbl")
            if node2_cur.fetchall() != [(1, '1'), (2, '2'), (3, None)]:
                sys.stderr.write("[(1, '1'), (2, '2'), (3, None)] expected")

            node2_cur.close()
            node2_conn.close()

            # cleanup: shutdown the monetdb servers and remove tempdir
            node1_proc.communicate()
            node2_proc.communicate()
