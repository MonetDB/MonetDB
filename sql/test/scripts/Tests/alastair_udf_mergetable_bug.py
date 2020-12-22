import os, socket, tempfile

from MonetDBtesting.sqltest import SQLTestCase
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

myport = freeport()

with tempfile.TemporaryDirectory() as farm_dir:
    os.mkdir(os.path.join(farm_dir, 'db1'))
    with process.server(mapiport=myport, dbname='db1',
                        dbfarm=os.path.join(farm_dir, 'db1'),
                        args=["--set", "gdk_nr_threads=2", "--forcemito"],
                        stdin=process.PIPE,
                        stdout=process.PIPE,
                        stderr=process.PIPE) as s:
        with SQLTestCase() as tc:
            tc.connect(username="monetdb", password="monetdb", port=myport, database='db1')
            tc.execute("create table tab1 (group_by_col int, index_col int, f float);").assertSucceeded()
            tc.execute("create table tab2 (index_col int, f float);").assertSucceeded()
            tc.execute("insert into tab1 values (1,1,1),(1,2,2),(2,1,3),(2,2,4),(3,1,5),(3,2,6);").assertSucceeded().assertRowCount(6)
            tc.execute("insert into tab2 values (1,111),(2,222),(3,333),(4,444);").assertSucceeded().assertRowCount(4)
            tc.execute("set optimizer='default_pipe';").assertSucceeded()
            tc.execute("select optimizer;").assertSucceeded().assertRowCount(1).assertDataResultMatch([("default_pipe",)])
            tc.execute("select tab1.group_by_col,SUM(fuse(cast (tab1.f as INT),cast (tab2.f as INT))) from tab2 inner join tab1 on tab1.index_col = tab2.index_col group by tab1.group_by_col;") \
                .assertSucceeded().assertRowCount(3).assertDataResultMatch([(1, 12884902221),(2, 30064771405),(3, 47244640589)])
            tc.execute("drop table tab1;").assertSucceeded()
            tc.execute("drop table tab2;").assertSucceeded()
        s.communicate()
