###
# Check indirect VIEW privilege:
#   A user with only(!) SELECT privilege on VIEWs of tables can use the VIEWs.
#   A user can view the columns of a VIEW to which it has been granted access.
# Check that if a user has CREATE VIEW privilege in one schema (i.e. is an
#   schema owner) but not in another schema, then the user can CREATE VIEWs
#   on tables in this schema but not in the other schema.
###

from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as mdb:
    mdb.connect(username="monetdb", password="monetdb")

    # my_user is the owner of my_schema, hence has all privileges here
    mdb.execute("CREATE ROLE my_role;").assertSucceeded()
    mdb.execute("CREATE SCHEMA my_schema AUTHORIZATION my_role;").assertSucceeded()
    mdb.execute("CREATE USER my_user WITH PASSWORD 'p1' NAME 'my_user' SCHEMA my_schema;").assertSucceeded()
    mdb.execute("GRANT my_role to my_user;").assertSucceeded()

    # someone else's schema, to parts of which my_user only has access when
    #   granted
    mdb.execute("CREATE SCHEMA your_schema;").assertSucceeded()
    mdb.execute("SET SCHEMA your_schema;").assertSucceeded()
    mdb.execute("CREATE TABLE your_table (name VARCHAR(10), birthday DATE, ssn CHAR(9));").assertSucceeded()
    mdb.execute("INSERT INTO your_table VALUES ('alice', '1980-01-01', 'AAAAAAAAA'), ('bob', '1970-01-01', '000000000');").assertRowCount(2)
    mdb.execute("CREATE VIEW your_view AS SELECT name, EXTRACT(YEAR FROM birthday) as yr, '********'||substring(ssn,9,9) as ssn FROM your_table;").assertSucceeded()
    mdb.execute("SELECT * FROM your_view;").assertSucceeded().assertDataResultMatch([('alice', 1980, '********A'), ('bob', 1970, '********0')])
    # grant indirect view right to my_user
    mdb.execute("GRANT SELECT on your_view to my_user;").assertSucceeded()


    with SQLTestCase() as tc:
        tc.connect(username="my_user", password="p1")

        # my_user can create tables, views in its own schema. Just a sanity check
        tc.execute("SET ROLE my_role;").assertSucceeded()
        tc.execute("CREATE TABLE my_schema.my_table (name VARCHAR(10), i INT);").assertSucceeded()
        tc.execute("CREATE VIEW my_schema.my_view AS SELECT * FROM my_schema.my_table;").assertSucceeded()
        mdb.execute("DROP VIEW my_schema.my_view;").assertSucceeded()
        mdb.execute("DROP TABLE my_schema.my_table;").assertSucceeded()

        # my_user can only indirectly select from your_view
        tc.execute("SELECT * FROM your_schema.your_table;").assertFailed(err_code="42000", err_message="SELECT: access denied for my_user to table 'your_schema.your_table'")
        tc.execute("SELECT * FROM your_schema.your_view;").assertSucceeded()\
            .assertDataResultMatch([('alice', 1980, '********A'), ('bob', 1970, '********0')])
        # my_user cannot create VIEWs on your_table
        tc.execute("CREATE VIEW your_view AS SELECT * FROM your_schema.your_table;").assertFailed(err_code="42000", err_message="SELECT: access denied for my_user to table 'your_schema.your_table'")

        # Check that we can revoke select from a whole view, but regrant select
        #   on some columns of the view
        mdb.execute("REVOKE SELECT on your_view FROM my_user;").assertSucceeded()
        tc.execute("SELECT * FROM your_schema.your_view;")\
            .assertFailed(err_code="42000", err_message="SELECT: access denied for my_user to view 'your_schema.your_view'")
        mdb.execute("GRANT SELECT (name) on your_view to my_user;").assertSucceeded()
        tc.execute("SELECT name FROM your_schema.your_view;").assertSucceeded()\
            .assertDataResultMatch([('alice',), ('bob',)])
        tc.execute("SELECT * FROM your_schema.your_view;").assertSucceeded()\
            .assertDataResultMatch([('alice',), ('bob',)])
        mdb.execute("GRANT SELECT (ssn) on your_view to my_user;").assertSucceeded()
        tc.execute("SELECT name, ssn FROM your_schema.your_view;").assertSucceeded()\
            .assertDataResultMatch([('alice', '********A'), ('bob', '********0')])
        tc.execute("SELECT * FROM your_schema.your_view;").assertSucceeded()\
            .assertDataResultMatch([('alice', '********A'), ('bob', '********0')])
        tc.execute("SELECT yr FROM your_schema.your_view;")\
            .assertFailed(err_code="42000", err_message="SELECT: identifier 'yr' unknown")

        # clean up
        mdb.execute("SET SCHEMA sys;").assertSucceeded()
        mdb.execute("DROP USER my_user;").assertSucceeded()
        mdb.execute("DROP ROLE my_role;").assertSucceeded()
        mdb.execute("DROP SCHEMA my_schema CASCADE;").assertSucceeded()
        mdb.execute("DROP SCHEMA your_schema CASCADE;").assertSucceeded()

