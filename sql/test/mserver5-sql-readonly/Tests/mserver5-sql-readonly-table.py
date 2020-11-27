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
        tc.execute("create table t1 (a int);").assertSucceeded()
        tc.execute("insert into t1 (a) values ( 1 );").assertSucceeded().assertRowCount(1)
        tc.execute("select * from t1;").assertSucceeded().assertDataResultMatch([(1,)])
        tc.execute("create table t2 (a int);").assertSucceeded()
        tc.execute("drop table t2;").assertSucceeded()
    s.communicate()

with process.server(args=["--readonly"],
                    stdin=process.PIPE,
                    stdout=process.PIPE,
                    stderr=process.PIPE) as s:
    with SQLTestCase() as tc:
        tc.connect(username="monetdb", password="monetdb")
        tc.execute("drop table t1;").assertFailed(err_message='Schema statements cannot be executed on a readonly database.')
        tc.execute("select * from t1;").assertSucceeded().assertDataResultMatch([(1,)])
        tc.execute("create table t2 (a int);").assertFailed(err_message='Schema statements cannot be executed on a readonly database.')
        tc.execute("create table t3 (a) as select * from t1 with data;").assertFailed(err_message='Schema statements cannot be executed on a readonly database.')
        tc.execute("create table t4 (a) as select * from t1 with no data;").assertFailed(err_message='Schema statements cannot be executed on a readonly database.')
        tc.execute("create table t5 ( like t1 );").assertFailed(err_message='Schema statements cannot be executed on a readonly database.')
        tc.execute("create temporary table t6 ( a int);").assertFailed(err_message='Schema statements cannot be executed on a readonly database.')
        tc.execute("create local temporary table t7 ( a int );").assertFailed(err_message='Schema statements cannot be executed on a readonly database.')
        tc.execute("create global temporary table t8 ( a int );").assertFailed(err_message='Schema statements cannot be executed on a readonly database.')
        tc.execute("insert into t1 (a) values ( 1 );").assertFailed(err_message='INSERT INTO: insert into table \'t1\' not allowed in readonly mode')
        tc.execute("update t1 set a = 2 where a = 1;").assertFailed(err_message='UPDATE: update table \'t1\' not allowed in readonly mode')
        tc.execute("delete from t1 where a = 1;").assertFailed(err_message='DELETE FROM: delete from table \'t1\' not allowed in readonly mode')
    s.communicate()
