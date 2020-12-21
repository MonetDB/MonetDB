###
# Tests for schema assignments and changes for users
###

from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as mdb:
    mdb.connect(username="monetdb", password="monetdb")
    mdb.execute("CREATE SCHEMA library;").assertSucceeded()
    mdb.execute("CREATE TABLE library.orders(price int, name VARCHAR(100));").assertSucceeded()

    mdb.execute("CREATE ROLE bankAdmin;").assertSucceeded()
    mdb.execute("CREATE SCHEMA bank AUTHORIZATION bankAdmin;").assertSucceeded()
    mdb.execute("CREATE TABLE bank.accounts(nr int, name VARCHAR(100));").assertSucceeded()

    mdb.execute("CREATE USER april WITH PASSWORD 'april' name 'april' schema bank;").assertSucceeded()
    mdb.execute("GRANT ALL ON bank.accounts to april;").assertSucceeded()
    mdb.execute("GRANT bankAdmin to april;").assertSucceeded()

    # Check that change the default schema for an unexisting user is not possible.
    mdb.execute('ALTER USER "april2" SET SCHEMA library;').assertFailed(err_code="42M32", err_message="ALTER USER: no such user 'april2'")
    # Check that change the default schema of a user to an unexisting schema is not possible.
    mdb.execute('ALTER USER "april" SET SCHEMA library2;').assertFailed(err_code="3F000", err_message="ALTER USER: no such schema 'library2'")

    with SQLTestCase() as tc:
        # Check that the admin can change the default schema of a user, and
        #   this will take effect the next time this user logs-in.
        tc.connect(username="april", password="april")
        tc.execute("SELECT current_schema;").assertDataResultMatch([('bank',)])
        tc.execute('SELECT * from accounts;').assertSucceeded()
        mdb.execute('ALTER USER "april" SET SCHEMA library').assertSucceeded()
        tc.connect(username="april", password="april")
        tc.execute("SELECT current_schema;").assertDataResultMatch([('library',)])

        # Check that after the schema change, the user no longer has direct access to tables in schema 'bank'
        tc.execute('SELECT * from accounts;').assertFailed(err_code="42S02", err_message="SELECT: no such table 'accounts'")
        # Check that after the schema change, the user still doesn't have access to tables in schema 'library'
        tc.execute('SELECT * from library.orders;').assertFailed(err_code="42000", err_message="SELECT: access denied for april to table 'library.orders'")

        # Check that drop a user that owns a schema is not possible.
        mdb.connect(username="monetdb", password="monetdb")
        mdb.execute('ALTER USER "april" SET SCHEMA bank;').assertSucceeded()
        mdb.execute('CREATE SCHEMA forAlice AUTHORIZATION april;').assertSucceeded()
        mdb.execute('DROP user april;').assertFailed(err_code="M1M05", err_message="DROP USER: 'april' owns a schema")

    # clean up
    mdb.execute('DROP TABLE library.orders;').assertSucceeded()
    mdb.execute('DROP TABLE bank.accounts;').assertSucceeded()

    mdb.execute('ALTER USER "april" SET SCHEMA sys;').assertSucceeded()
    mdb.execute('DROP SCHEMA forAlice;').assertSucceeded()
    mdb.execute('DROP SCHEMA bank;').assertSucceeded()
    mdb.execute('DROP SCHEMA library;').assertSucceeded()
    mdb.execute('DROP USER april;').assertSucceeded()
    mdb.execute('DROP ROLE bankAdmin;').assertSucceeded()

