###
# Check that a user can SELECT, INSERT, UPDATE, DELETE a table (created by
#   monetdb) for which the USER has GRANTs, while a user without those GRANTs
#   cannot
###

from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    tc.connect(username="monetdb", password="monetdb")
    tc.execute("CREATE USER my_user with password 'p1' name 'User with role' schema sys;").assertSucceeded()
    tc.execute("CREATE USER my_user2 with password 'p2' name 'User without role' schema sys;").assertSucceeded()
    tc.execute("CREATE SCHEMA my_schema;").assertSucceeded()
    tc.execute("CREATE table my_schema.test (i int, b bigint);").assertSucceeded()
    tc.execute("GRANT SELECT on table my_schema.test to my_user;").assertSucceeded()
    tc.execute("GRANT INSERT on table my_schema.test to my_user;").assertSucceeded()
    tc.execute("GRANT UPDATE on table my_schema.test to my_user;").assertSucceeded()
    tc.execute("GRANT DELETE on table my_schema.test to my_user;").assertSucceeded()

    tc.connect(username="my_user", password="p1")
    tc.execute("select * from my_schema.test;").assertSucceeded().assertRowCount(0)
    tc.execute("insert into my_schema.test values(1,1);").assertSucceeded().assertRowCount(1)
    tc.execute("update my_schema.test set b = 2;").assertSucceeded().assertRowCount(1)
    tc.execute("delete from my_schema.test;").assertSucceeded().assertRowCount(1)

    tc.connect(username="my_user2", password="p2")
    tc.execute("select * from my_schema.test;").assertFailed()
    tc.execute("insert into my_schema.test values(1,1);").assertFailed()
    tc.execute("update my_schema.test set b = 2;").assertFailed()
    tc.execute("delete from my_schema.test;").assertFailed()

    tc.connect(username="monetdb", password="monetdb")
    tc.execute("drop table my_schema.test;").assertSucceeded()
    tc.execute("drop schema my_schema;").assertSucceeded()
    tc.execute("drop user my_user;").assertSucceeded()
    tc.execute("drop user my_user2;").assertSucceeded()

