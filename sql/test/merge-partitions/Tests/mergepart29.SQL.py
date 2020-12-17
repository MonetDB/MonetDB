import os, socket, sys, tempfile, pymonetdb

try:
    from MonetDBtesting import process
except ImportError:
    import process


def freeport():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('', 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


with tempfile.TemporaryDirectory() as farm_dir:
    os.mkdir(os.path.join(farm_dir, 'db1'))
    node1_port = freeport()
    with process.server(mapiport=node1_port, dbname='db1',
                        dbfarm=os.path.join(farm_dir, 'db1'),
                        stdin=process.PIPE, stdout=process.PIPE,
                        stderr=process.PIPE) as node1_proc:
        node1_conn = pymonetdb.connect(database='db1', port=node1_port,
                                       autocommit=True)
        node1_cur = node1_conn.cursor()

        os.mkdir(os.path.join(farm_dir, 'db2'))
        node2_port = freeport()
        with process.server(mapiport=node2_port, dbname='db2',
                            dbfarm=os.path.join(farm_dir, 'db2'),
                            stdin=process.PIPE, stdout=process.PIPE,
                            stderr=process.PIPE) as node2_proc:
            node2_conn = pymonetdb.connect(database='db2', port=node2_port,
                                           autocommit=True)
            node2_cur = node2_conn.cursor()

            node2_cur.execute('CREATE TABLE "tb2" ("col1" int, "col2" int);')
            node2_cur.execute('INSERT INTO "tb2" VALUES (1, 1), (2, 2), (3, 3);')

            node1_cur.execute('CREATE MERGE TABLE "tb1" ("col1" int, "col2" int) PARTITION BY RANGE ON ("col1");')
            node1_cur.execute('CREATE REMOTE TABLE "tb2" ("col1" int, "col2" int) ON \'mapi:monetdb://localhost:'+str(node2_port)+'/db2\';')
            try:
                node1_cur.execute('ALTER TABLE "tb1" ADD TABLE "tb2" AS PARTITION FROM 0 TO 1;')  # error
                sys.stderr.write('Exception expected')
            except Exception as ex:
                if 'there are values in the column col1 outside the partition range' not in str(ex):
                    sys.stderr.write('Exception: there are values in the column col1 outside the partition range expected')
            node1_cur.execute('ALTER TABLE "tb1" ADD TABLE "tb2" AS PARTITION FROM 0 TO 100;')
            try:
                node1_cur.execute('INSERT INTO "tb1" VALUES (4, 4)')  # TODO, inserts on remote tables
                sys.stderr.write('Exception expected')
            except Exception as ex:
                if 'cannot insert remote table \'tb2\' from this server at the moment' not in str(ex):
                    sys.stderr.write('Exception: cannot insert remote table \'tb2\' from this server at the moment expected')
            node1_cur.execute('SELECT "col1", "col2" FROM "tb1";')
            if node1_cur.fetchall() != [(1, 1), (2, 2), (3, 3)]:
                sys.stderr.write('[(1, 1), (2, 2), (3, 3)] expected')

            out2, err2 = node2_proc.communicate()
            sys.stderr.write(err2)

        out1, err1 = node1_proc.communicate()
        sys.stderr.write(err1)

