###
# Grant sysadmin rights to a user.
# Verify that the user can assume the sysadmin role and CREATE new users, GRANT privileges and roles.
###

from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as mdb:
    mdb.connect(username="monetdb", password="monetdb")

    mdb.execute("CREATE SCHEMA library;").assertSucceeded()

    mdb.execute("CREATE USER alice WITH PASSWORD 'alice' name 'alice' schema library;").assertSucceeded()
    mdb.execute("CREATE USER april WITH PASSWORD 'april' name 'april' schema library;").assertSucceeded()


    mdb.execute("CREATE TABLE library.orders(price int, name VARCHAR(100));").assertSucceeded()

    with SQLTestCase() as tc:
        tc.connect(username="alice", password="alice")
        # alice is not a sysadmin yet
        tc.execute("SET ROLE sysadmin;").assertFailed(err_code='42000', err_message="Role (sysadmin) missing")
        tc.execute("CREATE USER may WITH PASSWORD 'may' NAME 'May' SCHEMA library;").assertFailed(err_code='42M31', err_message="Insufficient privileges to create user 'may'")
        tc.execute("GRANT ALL ON library.orders TO april;").assertFailed(err_code='01007', err_message="GRANT: Grantor 'alice' is not allowed to grant privileges for table 'orders'")
        # give alice sysadmin rights
        mdb.execute("GRANT sysadmin TO alice;").assertSucceeded()
        tc.execute("SET ROLE sysadmin;").assertSucceeded()
        tc.execute("CREATE USER may WITH PASSWORD 'may' NAME 'May' SCHEMA library;").assertSucceeded()
        tc.execute("DROP USER may;").assertSucceeded()

        with SQLTestCase() as tc2:
            # check that april can only SEL/INS/UPD/DEL the table after the GRANT ALL
            tc2.connect(username="april", password="april")
            tc2.execute("INSERT INTO library.orders VALUES (12, 'abc'), (42, 'def');").assertFailed(err_code='42000', err_message="INSERT INTO: insufficient privileges for user 'april' to insert into table 'orders'")
            tc2.execute("UPDATE library.orders SET price = price*2 WHERE price < 42;").assertFailed(err_code='42000', err_message="UPDATE: insufficient privileges for user 'april' to update table 'orders'")
            tc2.execute("DELETE FROM library.orders WHERE price = 42;").assertFailed(err_code='42000', err_message="DELETE FROM: insufficient privileges for user 'april' to delete from table 'orders'")
            tc2.execute("SELECT * FROM library.orders;").assertFailed(err_code='42000', err_message="SELECT: access denied for april to table 'library.orders'")

            tc.execute("GRANT ALL ON library.orders TO april;").assertSucceeded()

            tc2.execute("INSERT INTO library.orders VALUES (12, 'abc'), (42, 'def');").assertRowCount(2)
            tc2.execute("UPDATE library.orders SET price = price*2 WHERE price < 42;").assertRowCount(1)
            tc2.execute("DELETE FROM library.orders WHERE price = 42;").assertRowCount(1)
            tc2.execute("SELECT * FROM library.orders;").assertSucceeded().assertDataResultMatch([(24, 'abc')])
            tc2.execute("DROP TABLE library.orders;").assertFailed(err_code='42000', err_message="DROP TABLE: access denied for april to schema 'library'")

        # alice can only GRANT the role to another user if the role was granted
        #   to alice WITH ADMIN OPTION
        tc.execute("GRANT sysadmin TO april;").assertFailed(err_code='0P000', err_message="GRANT: Insufficient privileges to grant ROLE 'sysadmin'")
        mdb.execute("REVOKE sysadmin FROM alice;").assertSucceeded()
        mdb.execute("GRANT sysadmin TO alice WITH ADMIN OPTION;").assertSucceeded()
        tc.execute("GRANT sysadmin TO april;").assertSucceeded()

        # clean up
        mdb.execute("DROP TABLE library.orders;").assertSucceeded()
        mdb.execute("DROP SCHEMA library;").assertSucceeded()
        mdb.execute("DROP USER alice;").assertSucceeded()
        mdb.execute("DROP USER april;").assertSucceeded()

