###
# Grant SELECT and UPDATE to a user on two different columns.
# Verify the user can SELECT and UPDATE on the column it has permissions for.
# Verify that the user cannot SELECT nor UPDATE on the column it did not get permissions for.
###

from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    tc.connect(username="monetdb", password="monetdb")

    tc.execute("CREATE SCHEMA library;").assertSucceeded()
    tc.execute("CREATE TABLE library.orders(price int, name VARCHAR(100));").assertSucceeded()
    tc.execute("INSERT INTO library.orders VALUES (12, 'abc'), (42, 'def');").assertRowCount(2)

    tc.execute("CREATE USER alice WITH PASSWORD 'alice' name 'alice' schema library;").assertSucceeded()
    tc.execute("GRANT SELECT (price) ON library.orders TO alice;").assertSucceeded()
    tc.execute("GRANT UPDATE (name)  ON library.orders TO alice;").assertSucceeded()

    tc.connect(username="alice", password="alice")
    # alice can select price but no name, and it cannot update it
    tc.execute("SELECT price FROM orders;").assertSucceeded().assertRowCount(2)
    tc.execute("SELECT name FROM orders;").assertFailed(err_code='42000', err_message="SELECT: identifier 'name' unknown")
    tc.execute("SELECT * FROM orders;").assertSucceeded().assertDataResultMatch([(12,), (42,)])
    tc.execute("UPDATE orders SET price = 0;").assertFailed(err_code='42000', err_message="UPDATE: insufficient privileges for user 'alice' to update table 'orders' on column 'price'")

    # alice can update name
    tc.execute("UPDATE orders SET name = 'book title goes here';").assertSucceeded().assertRowCount(2)
    # FIXME: the following two queries currently fail due to the problems
    #   discussed in issue #7035.  May need to fix the expected error once that
    #   issue is fixed
    tc.execute("UPDATE orders SET name = name || 'book title goes here';").assertFailed(err_code='42000', err_message="SELECT: identifier 'name' unknown")
    # but not this update involving the 'price' column
    tc.execute("UPDATE orders SET name = 'new book title' where price = 12;").assertSucceeded().assertRowCount(1)

    # clean up
    tc.connect(username="monetdb", password="monetdb")
    tc.execute("DROP USER alice;").assertSucceeded()
    tc.execute("DROP TABLE library.orders;").assertSucceeded()
    tc.execute("DROP SCHEMA library;").assertSucceeded()
