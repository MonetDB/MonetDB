###
# Give a privilege to a USER, then remove it and give it again (possible).
# Assess that by granting one privilege, he only gets that one privilege.
# Assess that the privilege was indeed removed.
# Assess that it is possible to regrant the revoked privilege.
###

from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    tc.connect(username="monetdb", password="monetdb")
    tc.execute("""
        CREATE SCHEMA schemaTest;

        CREATE USER user_delete with password 'delete' name 'user can only delete' schema schemaTest;
        CREATE USER user_insert with password 'insert' name 'user can only insert' schema schemaTest;
        CREATE USER user_update with password 'update' name 'user can only update' schema schemaTest;
        CREATE USER user_select with password 'select' name 'user can only select' schema schemaTest;

        CREATE table schemaTest.testTable (v1 int, v2 int);

        INSERT into schemaTest.testTable values(1, 1);
        INSERT into schemaTest.testTable values(2, 2);
        INSERT into schemaTest.testTable values(3, 3);

        -- Grant rights.
        GRANT DELETE on table schemaTest.testTable to user_delete;
        GRANT INSERT on table schemaTest.testTable to user_insert;
        GRANT UPDATE on table schemaTest.testTable to user_update;
        GRANT SELECT on table schemaTest.testTable to user_delete;
        GRANT SELECT on table schemaTest.testTable to user_update;
        GRANT SELECT on table schemaTest.testTable to user_select;
    """).assertSucceeded()

    # When multiple queries are expected to fail, don't execute them in a batch
    # but individually, because, otherwise, the fact that a query actually
    # succeeds will be lost.
    tc.connect(username="user_delete", password="delete")
    tc.execute("DELETE FROM testTable where v1 = 2;").assertSucceeded()
    tc.execute("SELECT * FROM testTable;").assertSucceeded()
    tc.execute("UPDATE testTable set v1 = 2 where v2 = 7;").assertFailed(err_code="42000", err_message="UPDATE: insufficient privileges for user 'user_delete' to update table 'testtable' on column 'v1'")
    tc.execute("INSERT into testTable values (3, 3);").assertFailed(err_code="42000", err_message="INSERT INTO: insufficient privileges for user 'user_delete' to insert into table 'testtable'")

    tc.connect(username="user_update", password="update")
    tc.execute("UPDATE testTable set v1 = 2 where v2 = 7;").assertSucceeded()
    tc.execute("SELECT * FROM testTable;").assertSucceeded()
    tc.execute("INSERT into testTable values (3, 3);").assertFailed(err_code="42000", err_message="INSERT INTO: insufficient privileges for user 'user_update' to insert into table 'testtable'")
    tc.execute("DELETE FROM testTable where v1 = 2;").assertFailed(err_code="42000", err_message="DELETE FROM: insufficient privileges for user 'user_update' to delete from table 'testtable'")

    tc.connect(username="user_insert", password="insert")
    tc.execute("INSERT into testTable values (3, 3);").assertSucceeded()
    tc.execute("SELECT * FROM testTable;").assertFailed(err_code="42000", err_message="SELECT: access denied for user_insert to table 'schematest.testtable'")
    tc.execute("UPDATE testTable set v1 = 2 where v2 = 7;").assertFailed(err_code="42000", err_message="UPDATE: insufficient privileges for user 'user_insert' to update table 'testtable'")
    tc.execute("DELETE FROM testTable where v1 = 2;").assertFailed(err_code="42000", err_message="DELETE FROM: insufficient privileges for user 'user_insert' to delete from table 'testtable'")

    tc.connect(username="user_select", password="select")
    tc.execute("SELECT * FROM testTable;").assertSucceeded()
    tc.execute("INSERT into testTable values (3, 3);").assertFailed(err_code="42000", err_message="INSERT INTO: insufficient privileges for user 'user_select' to insert into table 'testtable'")
    tc.execute("UPDATE testTable set v1 = 2 where v2 = 7;").assertFailed(err_code="42000", err_message="UPDATE: insufficient privileges for user 'user_select' to update table 'testtable' on column 'v1'")
    tc.execute("DELETE FROM testTable where v1 = 2;").assertFailed(err_code="42000", err_message="DELETE FROM: insufficient privileges for user 'user_select' to delete from table 'testtable'")

    tc.connect(username="monetdb", password="monetdb")
    tc.execute("""
        REVOKE DELETE on schemaTest.testTable from user_delete;
        REVOKE INSERT on schemaTest.testTable from user_insert;
        REVOKE UPDATE on schemaTest.testTable from user_update;
        REVOKE SELECT on schemaTest.testTable from user_select;
    """).assertSucceeded()


    tc.connect(username='user_delete', password='delete')
    tc.execute("DELETE from testTable where v2 = 666;").assertFailed(err_code="42000", err_message="DELETE FROM: insufficient privileges for user 'user_delete' to delete from table 'testtable'")

    tc.connect(username='user_insert', password='insert')
    tc.execute("INSERT into testTable values (666, 666);").assertFailed(err_code="42000", err_message="INSERT INTO: insufficient privileges for user 'user_insert' to insert into table 'testtable'")

    tc.connect(username='user_update', password='update')
    tc.execute("UPDATE testTable set v1 = 666 where v2 = 666;").assertFailed(err_code="42000", err_message="UPDATE: insufficient privileges for user 'user_update' to update table 'testtable' on column 'v1'")

    tc.connect(username='user_select', password='select')
    tc.execute("SELECT * from schemaTest.testTable;").assertFailed(err_code="42000", err_message="SELECT: access denied for user_select to table 'schematest.testtable'")

    tc.connect(username='monetdb', password='monetdb')
    tc.execute("""
         -- Re-grant the rights.
         GRANT DELETE on table schemaTest.testTable to user_delete;
         GRANT INSERT on table schemaTest.testTable to user_insert;
         GRANT UPDATE on table schemaTest.testTable to user_update;
         GRANT SELECT on table schemaTest.testTable to user_select;
    """).assertSucceeded()

    tc.connect(username='user_delete', password='delete')
    tc.execute("DELETE from testTable where v1 = 42;").assertSucceeded()

    tc.connect(username='user_insert', password='insert')
    tc.execute("INSERT into testTable values (42, 42);").assertSucceeded()

    tc.connect(username='user_update', password='update')
    tc.execute("UPDATE testTable set v1 = 42 where v2 = 42;").assertSucceeded()

    tc.connect(username='user_select', password='select')
    tc.execute("SELECT * FROM testTable where v1 = 42;").assertSucceeded()

