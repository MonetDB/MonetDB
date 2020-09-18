import os
import socket
import sys
import tempfile

import pymonetdb

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
        node1_cur.execute("insert into lala select 13,85,98,15,113,2")
        node1_cur.execute("select * from lala")
        print(node1_cur.fetchall())
        node1_cur.execute("select * from mudf((select * from lala))")
        print(node1_cur.fetchall())
        node1_cur.execute("select * from mudf2((select * from lala))")
        print(node1_cur.fetchall())

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
            print(node2_cur.fetchall())
            node2_cur.execute("select * from mudf((select * from fofo))")
            print(node2_cur.fetchall())
            node2_cur.execute("select * from mudf2((select * from fofo))")
            print(node2_cur.fetchall())

            # cleanup: shutdown the monetdb servers and remove tempdir
            out, err = node1_proc.communicate()
            sys.stderr.write(err)

            out, err = node2_proc.communicate()
            sys.stderr.write(err)
