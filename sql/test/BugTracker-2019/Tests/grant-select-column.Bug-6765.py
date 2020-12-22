###
# Check that when a user is granted (select) access to some columns in a table,
#   the user can indeed access those columns.
# In addition, check that after the access to some of the granted columns has
#   been revoked, the user can access the remaining columns.
###


from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as mdb:
    mdb.connect(username="monetdb", password="monetdb")
    mdb.execute("CREATE schema myschema;").assertSucceeded()
    mdb.execute("CREATE USER myuser WITH UNENCRYPTED PASSWORD 'Test123' NAME 'Hulk' SCHEMA myschema;").assertSucceeded()
    mdb.execute("SET SCHEMA myschema;").assertSucceeded()
    mdb.execute("CREATE TABLE test (id integer, name varchar(20), address varchar(20));").assertSucceeded()
    mdb.execute("INSERT INTO test (id, name,address) VALUES (1,'Tom', 'planet'),(2,'Karen', 'earth');").assertSucceeded()

    with SQLTestCase() as tc:
        tc.connect(username="myuser", password="Test123")
        # 'myuser' cannot SELECT before GRANT and after REVOKE, can SELECT after GRANT
        tc.execute("select * from test").assertFailed(err_code='42000', err_message="SELECT: access denied for myuser to table 'myschema.test'")
        mdb.execute("GRANT SELECT ON test TO myuser;").assertSucceeded()
        tc.execute("select * from test").assertRowCount(2)
        mdb.execute("REVOKE SELECT ON test FROM myuser;").assertSucceeded()
        tc.execute("select * from test").assertFailed(err_code='42000', err_message="SELECT: access denied for myuser to table 'myschema.test'")

        # 'myuser' can SELECT test(id)
        mdb.execute("GRANT SELECT (id) ON test TO myuser;").assertSucceeded()
        tc.execute("select id from test").assertDataResultMatch([(1,),(2,)])
        tc.execute("select name from test").assertFailed(err_code='42000', err_message="SELECT: identifier 'name' unknown")
        tc.execute("select address from test").assertFailed(err_code='42000', err_message="SELECT: identifier 'address' unknown")
        tc.execute("select * from test").assertDataResultMatch([(1,),(2,)])

        # 'myuser' can SELECT test(id, address)
        mdb.execute("GRANT SELECT (address) ON test TO myuser;").assertSucceeded()
        tc.execute("select id from test").assertDataResultMatch([(1,),(2,)])
        tc.execute("select name from test").assertFailed(err_code='42000', err_message="SELECT: identifier 'name' unknown")
        tc.execute("select address from test").assertDataResultMatch([('planet',),('earth',)])
        tc.execute("select * from test").assertDataResultMatch([(1,'planet'),(2,'earth')])

        # 'myuser' can only SELECT test(address)
        mdb.execute("REVOKE SELECT (id) ON test FROM myuser;").assertSucceeded()
        tc.execute("select id from test").assertFailed(err_code='42000', err_message="SELECT: identifier 'id' unknown")
        tc.execute("select name from test").assertFailed(err_code='42000', err_message="SELECT: identifier 'name' unknown")
        tc.execute("select address from test").assertDataResultMatch([('planet',),('earth',)])
        tc.execute("select * from test").assertDataResultMatch([('planet',),('earth',)])

    # clean up
    mdb.execute("SET SCHEMA sys;").assertSucceeded()
    mdb.execute("DROP USER myuser;").assertSucceeded()
    mdb.execute("DROP SCHEMA myschema CASCADE;").assertSucceeded()
