from MonetDBtesting.sqltest import SQLTestCase

try:
    from MonetDBtesting import process
except ImportError:
    import process


with process.server(args=[],
                    stdin=process.PIPE,
                    stdout=process.PIPE,
                    stderr=process.PIPE) as s:
    with SQLTestCase() as tc:
        tc.connect(username="monetdb", password="monetdb")
        tc.execute("select * from t1;").assertSucceeded().assertDataResultMatch([(1,)])
        tc.execute("create view v1 as select * from t1;").assertSucceeded()
        tc.execute("create view v2 as select * from t1;").assertSucceeded()
        tc.execute("drop view v2;").assertSucceeded()
        tc.execute("insert into v1 (a) values ( 2 );").assertFailed(err_message='INSERT INTO: cannot insert into view \'v1\'')
        tc.execute("update v1 set a = 3 where a = 2;").assertFailed(err_message='UPDATE: cannot update view \'v1\'')
        tc.execute("delete from v1 where a = 3;").assertFailed(err_message='DELETE FROM: cannot delete from view \'v1\'')
    s.communicate()

with process.server(args=["--readonly"],
                    stdin=process.PIPE,
                    stdout=process.PIPE,
                    stderr=process.PIPE) as s:
    with SQLTestCase() as tc:
        tc.connect(username="monetdb", password="monetdb")
        tc.execute("select * from t1;").assertSucceeded().assertDataResultMatch([(1,)])
        tc.execute("create view v2 as select * from t1;").assertFailed(err_message='Schema statements cannot be executed on a readonly database.')
        tc.execute("drop view v1;").assertFailed(err_message='Schema statements cannot be executed on a readonly database.')
        tc.execute("select * from v1;").assertSucceeded()
        tc.execute("insert into v1 (a) values ( 1 );").assertFailed(err_message='INSERT INTO: cannot insert into view \'v1\'')
        tc.execute("update v1 set a = 2 where a = 1;").assertFailed(err_message='UPDATE: cannot update view \'v1\'')
        tc.execute("delete from v1 where a = 1;").assertFailed(err_message='DELETE FROM: cannot delete from view \'v1\'')
    s.communicate()
