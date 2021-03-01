from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    tc.execute("create table foo(id bigserial, value int);").assertSucceeded()
    tc.execute("create table bar(id bigserial, value int);").assertSucceeded()
    # test cross product
    tc.execute("insert into foo(value) (select * from generate_series(0,10000));").assertSucceeded()
    tc.execute("insert into bar(value) (select * from generate_series(0,10000));").assertSucceeded()
    tc.execute("call sys.setquerytimeout(1); select * from foo, bar;")\
        .assertFailed(err_code="HYT00", err_message="Query aborted due to timeout")
    # test mergejoin_int with bigger data set
    tc.execute("insert into foo(value) (select * from generate_series(0,10000000));").assertSucceeded()
    tc.execute("insert into bar(value) (select * from generate_series(0,10000000));").assertSucceeded()
    tc.execute("call sys.setquerytimeout(1); select * from foo as f join bar b on f.value=b.value;")\
        .assertFailed(err_code="HYT00", err_message="Query aborted due to timeout")
