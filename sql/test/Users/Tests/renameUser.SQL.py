###
# Check various aspects of renaming users
###

from MonetDBtesting.sqltest import SQLTestCase
import logging

logging.basicConfig(level=logging.FATAL)

with SQLTestCase() as mdb:
    with SQLTestCase() as tc2:
        mdb.connect(username="monetdb", password="monetdb")
        # prepare a user 'april' as the owner of schema 'bank'
        mdb.execute("CREATE ROLE bankAdmin;").assertSucceeded()
        mdb.execute("CREATE SCHEMA bank AUTHORIZATION bankAdmin;").assertSucceeded()
        mdb.execute("CREATE USER april WITH PASSWORD 'april' NAME 'april' SCHEMA bank;").assertSucceeded()
        mdb.execute("GRANT bankAdmin to april;").assertSucceeded()
        mdb.execute("CREATE TABLE bank.accounts(nr int, name VARCHAR(100));").assertSucceeded()
        # just a sanity check
        tc2.connect(username="april", password="april")
        tc2.execute("SET ROLE bankAdmin;").assertSucceeded()

        # Check that:
        #   the admin can rename a user (from A to B);
        #   after the rename, user B can log in but not user A
        mdb.execute("ALTER USER april RENAME TO april2;").assertSucceeded()
        tc2.connect(username="april", password="april")
        tc2.execute("SET ROLE bankAdmin;").assertFailed(err_code=None, err_message="InvalidCredentialsException:checkCredentials:invalid credentials for user 'april'")
        tc2.connect(username="april2", password="april")
        tc2.execute("SET ROLE bankAdmin;").assertSucceeded()
        # Check that the renamed user (B) still has its rights.
        tc2.execute("SET role bankAdmin;").assertSucceeded()
        tc2.execute("DROP TABLE accounts;").assertSucceeded()
        tc2.execute("CREATE TABLE accounts(nr int, name VARCHAR(100));").assertSucceeded()
        tc2.execute("INSERT INTO accounts VALUES (24, 'abc'), (42, 'xyz');").assertRowCount(2)
        tc2.execute("UPDATE accounts SET nr = 666;").assertRowCount(2)
        tc2.execute("DELETE FROM accounts WHERE name = 'abc';").assertRowCount(1)
        tc2.execute("SELECT * from bank.accounts;").assertRowCount(1)

        # Check that:
        #   the admin can create another user with the old name (A);
        #   the new user (A) cannot make use of the role assign to the inital user (now B).
        mdb.execute("CREATE USER april with password 'april' name 'second' schema bank;").assertSucceeded()
        tc2.connect(username="april", password="april")
        tc2.execute("SET role bankAdmin;").assertFailed(err_code="42000", err_message="Role (bankadmin) missing")
        tc2.execute("DROP TABLE accounts;").assertFailed(err_code="42000", err_message="DROP TABLE: access denied for april to schema 'bank'")
        tc2.execute("CREATE TABLE accounts2(nr int, name VARCHAR(100));").assertFailed(err_code="42000", err_message="CREATE TABLE: insufficient privileges for user 'april' in schema 'bank'")
        tc2.execute("INSERT INTO accounts VALUES (24, 'abc'), (42, 'xyz');").assertFailed(err_code="42000", err_message="INSERT INTO: insufficient privileges for user 'april' to insert into table 'accounts'")
        tc2.execute("UPDATE accounts SET nr = 666;").assertFailed(err_code="42000", err_message="UPDATE: insufficient privileges for user 'april' to update table 'accounts' on column 'nr'")
        tc2.execute("DELETE FROM accounts WHERE name = 'abc';").assertFailed(err_code="42000", err_message="DELETE FROM: insufficient privileges for user 'april' to delete from table 'accounts'")
        tc2.execute("SELECT * from bank.accounts;").assertFailed(err_code="42000", err_message="SELECT: access denied for april to table 'bank.accounts'")
        # Check that a user with no special permissions cannot rename users.
        # FIXME: might need to change the err_message (see issue #7037)
        tc2.execute("ALTER USER april2 RENAME TO april3;")\
            .assertFailed(err_code="M1M05", err_message="ALTER USER: insufficient privileges to rename user 'april2'")

        mdb.connect(username='monetdb', password='monetdb')
        # Check that the admin cannot:
        #   rename a user with an already existing name;
        mdb.execute('ALTER USER "april2" RENAME TO "april";')\
            .assertFailed(err_code="42M31", err_message="ALTER USER: user 'april' already exists")
        mdb.execute("drop user april;").assertSucceeded()
        mdb.execute('ALTER USER "april2" RENAME TO "april";').assertSucceeded()
        #   rename an unexisting user;
        mdb.execute('ALTER USER "april5" RENAME TO "april2";')\
            .assertFailed(err_code="42M32", err_message="ALTER USER: no such user 'april5'")
        mdb.execute("drop user april2;")\
            .assertFailed(err_code="M0M27", err_message="DROP USER: no such user: 'april2'")
        #   create a user on a non-existing schema;
        mdb.execute("CREATE USER april2 with password 'april' name 'second april, no rights' schema library2;")\
            .assertFailed(err_code="3F000", err_message="CREATE USER: no such schema 'library2'")
        #   create a user with a name of an existing user.
        mdb.execute("CREATE USER april with password 'april' name 'second april, no rights' schema library;")\
            .assertFailed(err_code="42M31", err_message="CREATE USER: user 'april' already exists")

    # clean up
    mdb.execute("DROP TABLE bank.accounts;").assertSucceeded()
    mdb.execute("DROP USER april;").assertSucceeded()
    mdb.execute("DROP SCHEMA bank;").assertSucceeded()
    mdb.execute("DROP ROLE bankAdmin;").assertSucceeded()

