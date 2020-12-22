from MonetDBtesting.sqltest import SQLTestCase
import os, socket, sys, tempfile
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

class server_start(process.server):
    def __init__(self, args):
        super().__init__(mapiport=myport, dbname='db1',
                         dbfarm=os.path.join(farm_dir, 'db1'),
                         stdin=process.PIPE, stdout=process.PIPE,
                         stderr=process.PIPE)

with tempfile.TemporaryDirectory() as farm_dir:
    os.mkdir(os.path.join(farm_dir, 'db1'))
    myport = freeport()

    with server_start(["--set", "sql_debug=64"]) as srv:
        with SQLTestCase() as tc:
            tc.connect(username="monetdb", password="monetdb", port=myport, database='db1')
            tc.execute("create table t1999354a(ra float, \"dec\" int);").assertSucceeded()
            tc.execute("""
                CREATE FUNCTION f2(deg float, truncat int , precision int)
                RETURNS varchar(32)
                BEGIN
                DECLARE
                d float,
                nd int,
                np int,
                q varchar(10),
                t varchar(16);

                SET t = '00:00:00.0';
                IF (precision < 1)
                THEN SET precision = 1;
                END IF;
                IF (precision > 10)
                THEN SET precision = 10;
                END IF;
                SET np = 0;
                WHILE (np < precision-1) DO
                SET t = t||'0';
                SET np = np + 1;
                END WHILE;
                SET d = ABS(deg/15.0);
                -- degrees
                SET nd = FLOOR(d);
                SET q = LTRIM(CAST(nd as varchar(2)));
                SET t = MS_STUFF(t,3-LENGTH(q),LENGTH(q), q);
                -- minutes
                SET d = 60.0 * (d-nd);
                SET nd = FLOOR(d);
                SET q = LTRIM(CAST(nd as varchar(4)));
                SET t = MS_STUFF(t,6-LENGTH(q),LENGTH(q), q);
                -- seconds
                SET d = MS_ROUND( 60.0 * (d-nd),precision,truncat );
                SET t = MS_STUFF(t,10+precision-LENGTH(q),LENGTH(q), q);
                RETURN(t);
                END;
            """).assertSucceeded()

            tc.execute("SELECT f2(1,2,3);").assertSucceeded().assertRowCount(1).assertDataResultMatch([("00:04:00.004",)])
            tc.execute("SELECT f2(p.ra,8,p.\"dec\") FROM t1999354a as p;").assertSucceeded().assertRowCount(0).assertDataResultMatch([])
            tc.execute("drop function f2;").assertSucceeded()
            tc.execute("drop table t1999354a;").assertSucceeded()
        srv.communicate()
