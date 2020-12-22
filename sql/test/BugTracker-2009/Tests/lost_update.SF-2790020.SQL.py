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

class server(process.server):
    def __init__(self):
        super().__init__(mapiport=myport, dbname='db1',
                         dbfarm=os.path.join(farm_dir, 'db1'),
                         stdin=process.PIPE,
                         stdout=process.PIPE, stderr=process.PIPE)

    def server_stop(self):
        self.communicate()

myport = freeport()

with tempfile.TemporaryDirectory() as farm_dir:
    os.mkdir(os.path.join(farm_dir, 'db1'))
    with server() as s:
        with SQLTestCase() as tc:
            tc.connect(username="monetdb", password="monetdb", port=myport, database="db1")
            tc.execute("create table lost_update_t2 (a int);").assertSucceeded()
            tc.execute("insert into lost_update_t2 values (1);").assertSucceeded().assertRowCount(1)
            tc.execute("update lost_update_t2 set a = 2;").assertSucceeded().assertRowCount(1)
        s.server_stop()
    with server() as s:
        with SQLTestCase() as tc:
            tc.connect(username="monetdb", password="monetdb", port=myport, database="db1")
            tc.execute("update lost_update_t2 set a = 3;").assertSucceeded().assertRowCount(1)
            tc.execute("create table lost_update_t1 (a int);").assertSucceeded()
            tc.execute("insert into lost_update_t1 values (1);").assertSucceeded().assertRowCount(1)
            tc.execute("insert into lost_update_t1 (select * from lost_update_t1);").assertSucceeded().assertRowCount(1)
            tc.execute("insert into lost_update_t1 (select * from lost_update_t1);").assertSucceeded().assertRowCount(2)
            tc.execute("insert into lost_update_t1 (select * from lost_update_t1);").assertSucceeded().assertRowCount(4)
            tc.execute("insert into lost_update_t1 (select * from lost_update_t1);").assertSucceeded().assertRowCount(8)
            tc.execute("insert into lost_update_t1 (select * from lost_update_t1);").assertSucceeded().assertRowCount(16)
            tc.execute("insert into lost_update_t1 (select * from lost_update_t1);").assertSucceeded().assertRowCount(32)
            tc.execute("insert into lost_update_t1 (select * from lost_update_t1);").assertSucceeded().assertRowCount(64)
            tc.execute("insert into lost_update_t1 (select * from lost_update_t1);").assertSucceeded().assertRowCount(128)
            tc.execute("insert into lost_update_t1 (select * from lost_update_t1);").assertSucceeded().assertRowCount(256)
            tc.execute("insert into lost_update_t1 (select * from lost_update_t1);").assertSucceeded().assertRowCount(512)
            tc.execute("insert into lost_update_t1 (select * from lost_update_t1);").assertSucceeded().assertRowCount(1024)
            tc.execute("update lost_update_t1 set a = 2;").assertSucceeded().assertRowCount(2048)
            tc.execute("call sys.flush_log();").assertSucceeded()
        s.server_stop()
    with server() as s:
        with SQLTestCase() as tc:
            tc.connect(username="monetdb", password="monetdb", port=myport, database="db1")
            tc.execute("select a from lost_update_t2;").assertSucceeded().assertRowCount(1).assertDataResultMatch([(3,)])
        s.server_stop()
    with server() as s:
        with SQLTestCase() as tc:
            tc.connect(username="monetdb", password="monetdb", port=myport, database="db1")
            tc.execute("drop table lost_update_t1;").assertSucceeded()
            tc.execute("drop table lost_update_t2;").assertSucceeded()
        s.server_stop()
