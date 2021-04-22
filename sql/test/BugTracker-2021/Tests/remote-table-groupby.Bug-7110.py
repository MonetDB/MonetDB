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

        node1_cur.execute("CREATE TABLE keyword_test (toc_no String null,myname String null)")
        node1_cur.execute("insert into keyword_test values('A000000009', 'AAAA'),('A000000010', 'BBBB'),('A000000011', 'CCCC'),('A000000012', 'DDDD'),('A000000013', 'EEEE'),('A000000014', 'AAAA'),('A000000015', 'DDDD'),('A000000016', 'AAAA')")
        node1_cur.execute("select * from keyword_test order by myname")
        if node1_cur.fetchall() != [('A000000009', 'AAAA'), ('A000000014', 'AAAA'), ('A000000016', 'AAAA'), ('A000000010', 'BBBB'), ('A000000011', 'CCCC'), ('A000000012', 'DDDD'), ('A000000015', 'DDDD'), ('A000000013', 'EEEE')]:
            sys.stderr.write("[('A000000009', 'AAAA'), ('A000000014', 'AAAA'), ('A000000016', 'AAAA'), ('A000000010', 'BBBB'), ('A000000011', 'CCCC'), ('A000000012', 'DDDD'), ('A000000015', 'DDDD'), ('A000000013', 'EEEE')] expected")
        node1_cur.execute("select '*' as category, count(*) as cnt from keyword_test group by category")
        if node1_cur.fetchall() != [('*', 8)]:
            sys.stderr.write("[('*', 8)] expected")

        node2_port = freeport()
        with process.server(mapiport=node2_port, dbname='node2',
                            dbfarm=os.path.join(farm_dir, 'node2'),
                            stdin=process.PIPE, stdout=process.PIPE,
                            stderr=process.PIPE) as node2_proc:
            node2_conn = pymonetdb.connect(database='node2', port=node2_port, autocommit=True)
            node2_cur = node2_conn.cursor()

            node2_cur.execute("CREATE REMOTE TABLE keyword_test (toc_no String null,myname String null) on 'mapi:monetdb://localhost:{}/node1/sys/keyword_test'".format(node1_port))
            node2_cur.execute("select * from keyword_test order by myname")
            if node2_cur.fetchall() != [('A000000009', 'AAAA'), ('A000000014', 'AAAA'), ('A000000016', 'AAAA'), ('A000000010', 'BBBB'), ('A000000011', 'CCCC'), ('A000000012', 'DDDD'), ('A000000015', 'DDDD'), ('A000000013', 'EEEE')]:
                sys.stderr.write("[('A000000009', 'AAAA'), ('A000000014', 'AAAA'), ('A000000016', 'AAAA'), ('A000000010', 'BBBB'), ('A000000011', 'CCCC'), ('A000000012', 'DDDD'), ('A000000015', 'DDDD'), ('A000000013', 'EEEE')] expected")
            node2_cur.execute("select '*' as category, count(*) as cnt from keyword_test group by category")
            if node2_cur.fetchall() != [('*', 8)]:
                sys.stderr.write("[('*', 8)] expected")

            # cleanup: shutdown the monetdb servers and remove tempdir
            out, err = node1_proc.communicate()
            sys.stderr.write(err)

            out, err = node2_proc.communicate()
            sys.stderr.write(err)
