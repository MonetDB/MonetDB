from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    tc.connect(username="monetdb", password="monetdb")
    tc.execute("CREATE TABLE t1(id int, name varchar(1024), age int, PRIMARY KEY(id));").assertSucceeded()
    tc.execute("CREATE VIEW v1 as select id, age from t1 where name like 'monet%';").assertSucceeded()
    tc.execute("ALTER TABLE v1 DROP COLUMN age;").assertFailed(err_message="ALTER TABLE: cannot drop column from VIEW 'v1'")
    tc.execute("CREATE TRIGGER trigger_test AFTER INSERT ON v1 INSERT INTO t2 values(1,23);").assertFailed(err_message="CREATE TRIGGER: cannot create trigger on view 'v1'")
    tc.execute("CREATE INDEX id_age_index ON v1(id,age);").assertFailed(err_message="CREATE INDEX: cannot create index on view 'v1'")
    tc.execute("DROP view v1;").assertSucceeded()
    tc.execute("DROP table t1;").assertSucceeded()
