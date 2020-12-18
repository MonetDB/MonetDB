###
# Grant a user the right to regrant a privilege.
# Verify that the user can regrant the privilege.
###

from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as mdb:
    mdb.connect(username="monetdb", password="monetdb")

    mdb.execute("CREATE SCHEMA library;").assertSucceeded()
    mdb.execute("CREATE SCHEMA bank;").assertSucceeded()
    mdb.execute("CREATE TABLE bank.loans(nr int, amount int);").assertSucceeded()

    mdb.execute("CREATE USER alice WITH PASSWORD 'alice' name 'alice' schema library;").assertSucceeded()
    mdb.execute("CREATE USER april WITH PASSWORD 'april' name 'april' schema library;").assertSucceeded()

    mdb.execute("GRANT SELECT ON bank.loans TO april WITH GRANT OPTION;").assertSucceeded()
    mdb.execute("GRANT INSERT ON bank.loans TO april WITH GRANT OPTION;").assertSucceeded()
    mdb.execute("GRANT UPDATE ON bank.loans TO april WITH GRANT OPTION;").assertSucceeded()
    mdb.execute("GRANT DELETE ON bank.loans TO april WITH GRANT OPTION;").assertSucceeded()

    with SQLTestCase() as tc:
        tc.connect(username="alice", password="alice")
        # alice doesn't have access to bank.loans yet
        tc.execute("INSERT INTO bank.loans VALUES (12, 127), (42, 8191);").assertFailed(err_code='42000', err_message="INSERT INTO: insufficient privileges for user 'alice' to insert into table 'loans'")
        tc.execute("UPDATE bank.loans SET amount = amount - 100 WHERE nr = 42;").assertFailed(err_code='42000', err_message="UPDATE: insufficient privileges for user 'alice' to update table 'loans'")
        tc.execute("DELETE FROM bank.loans WHERE nr = 12;").assertFailed(err_code='42000', err_message="DELETE FROM: insufficient privileges for user 'alice' to delete from table 'loans'")
        tc.execute("SELECT * FROM bank.loans;").assertFailed(err_code='42000', err_message="SELECT: access denied for alice to table 'bank.loans'")
        # let april grant alice all rights
        tc.connect(username="april", password="april")
        tc.execute("GRANT SELECT ON bank.loans TO alice WITH GRANT OPTION;").assertSucceeded()
        tc.execute("GRANT INSERT ON bank.loans TO alice WITH GRANT OPTION;").assertSucceeded()
        tc.execute("GRANT UPDATE ON bank.loans TO alice WITH GRANT OPTION;").assertSucceeded()
        tc.execute("GRANT DELETE ON bank.loans TO alice WITH GRANT OPTION;").assertSucceeded()

        tc.execute("INSERT INTO bank.loans VALUES (12, 127), (42, 8191);").assertRowCount(2)
        tc.execute("UPDATE bank.loans SET amount = amount - 100 WHERE nr = 42;").assertRowCount(1)
        tc.execute("DELETE FROM bank.loans WHERE nr = 12;").assertRowCount(1)
        tc.execute("SELECT * FROM bank.loans;").assertSucceeded().assertDataResultMatch([(42, 8091)])

        # clean up
        mdb.execute("DROP TABLE bank.loans;").assertSucceeded()
        mdb.execute("DROP USER april;").assertSucceeded()
        mdb.execute("DROP USER alice;").assertSucceeded()
        mdb.execute("DROP SCHEMA bank;").assertSucceeded()
        mdb.execute("DROP SCHEMA library;").assertSucceeded()

