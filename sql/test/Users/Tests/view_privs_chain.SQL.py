###
# Check indirect VIEW privilege:
#   check that GRANT SELECT on <view> works correctly in assigning various
#   chains of views and tables to different users
###
from MonetDBtesting.sqltest import SQLTestCase
from decimal import Decimal

with SQLTestCase() as mdb:
    mdb.connect(username="monetdb", password="monetdb")

    # create two users in my_schema
    mdb.execute("CREATE ROLE my_role;").assertSucceeded()
    mdb.execute("CREATE SCHEMA my_schema AUTHORIZATION my_role;").assertSucceeded()
    mdb.execute("CREATE USER usr1 WITH PASSWORD 'p1' NAME 'usr1' SCHEMA my_schema;").assertSucceeded()
    mdb.execute("CREATE USER usr2 WITH PASSWORD 'p2' NAME 'usr2' SCHEMA my_schema;").assertSucceeded()
    mdb.execute("GRANT my_role to usr1;").assertSucceeded()
    mdb.execute("GRANT my_role to usr2;").assertSucceeded()

    # create tables and views in another schema
    mdb.execute("CREATE SCHEMA your_schema;").assertSucceeded()
    mdb.execute("SET SCHEMA your_schema;").assertSucceeded()
    mdb.execute("CREATE TABLE person (name VARCHAR(10), birthday DATE, ssn CHAR(9));").assertSucceeded()
    mdb.execute("INSERT INTO person VALUES ('alice', '1980-01-01', 'AAAAAAAAA'), ('bob', '1970-01-01', '000000000');").assertRowCount(2)
    mdb.execute("CREATE TABLE employee (name VARCHAR(10), salary DECIMAL(10,2));").assertSucceeded()
    mdb.execute("INSERT INTO employee VALUES ('alice', 888.42), ('bob', 444.42);").assertRowCount(2)

    # v1 = join(table, table)
    mdb.execute("""
        CREATE VIEW v1 AS SELECT
          p.name,
          EXTRACT(YEAR FROM birthday) as yr,
          '********'||substring(ssn,9,9) as ssn,
          ifthenelse((salary > 500), 'high', 'low') as salary
        FROM person p, employee e
        WHERE p.name = e.name;
    """).assertSucceeded()
    mdb.execute("SELECT * FROM v1;").assertSucceeded()\
        .assertDataResultMatch([('alice', 1980, '********A', 'high'), ('bob', 1970, '********0', 'low')])

    # v2 = join(table, view)
    mdb.execute("""
        CREATE VIEW v2 AS SELECT v1.name, v1.ssn, e.salary
        FROM v1, employee e
        WHERE v1.name = e.name;
    """).assertSucceeded()
    mdb.execute("SELECT * FROM v2;").assertSucceeded()\
        .assertDataResultMatch([('alice', '********A', Decimal('888.42')), ('bob', '********0', Decimal('444.42'))])

    # v3 = join(view, view)
    mdb.execute("""
        CREATE VIEW v3 AS SELECT v1.name, v1.yr, v2.salary
        FROM v1, v2
        WHERE v1.name = v2.name;
    """).assertSucceeded()
    mdb.execute("SELECT * FROM v3;").assertSucceeded()\
        .assertDataResultMatch([('alice', 1980, Decimal('888.42')), ('bob', 1970, Decimal('444.42'))])

    # v4 = project(view)
    mdb.execute("""
        CREATE VIEW v4 AS SELECT yr,
          ifthenelse((salary > 500), 'high', 'low') as salary
        FROM v3
    """).assertSucceeded()
    mdb.execute("SELECT * FROM v4;").assertSucceeded()\
        .assertDataResultMatch([(1980, 'high'), (1970, 'low')])
    mdb.execute("SET SCHEMA sys;").assertSucceeded()

    # usr1 has no access to the tables and gets access to views top-down
    with SQLTestCase() as tc:
        tc.connect(username="usr1", password="p1")

        mdb.execute("GRANT SELECT on your_schema.v4 to usr1;").assertSucceeded()
        tc.execute("SELECT * FROM your_schema.v4;").assertSucceeded()\
            .assertDataResultMatch([(1980, 'high'), (1970, 'low')])
        tc.execute("SELECT * FROM your_schema.v3;")\
            .assertFailed(err_code="42000", err_message="SELECT: access denied for usr1 to view 'your_schema.v3'")
        tc.execute("SELECT * FROM your_schema.v2;")\
            .assertFailed(err_code="42000", err_message="SELECT: access denied for usr1 to view 'your_schema.v2'")
        tc.execute("SELECT * FROM your_schema.v1;")\
            .assertFailed(err_code="42000", err_message="SELECT: access denied for usr1 to view 'your_schema.v1'")

        mdb.execute("GRANT SELECT on your_schema.v3 to usr1;").assertSucceeded()
        tc.execute("SELECT * FROM your_schema.v3;").assertSucceeded()\
            .assertDataResultMatch([('alice', 1980, Decimal('888.42')), ('bob', 1970, Decimal('444.42'))])
        tc.execute("SELECT * FROM your_schema.v2;")\
            .assertFailed(err_code="42000", err_message="SELECT: access denied for usr1 to view 'your_schema.v2'")
        tc.execute("SELECT * FROM your_schema.v1;")\
            .assertFailed(err_code="42000", err_message="SELECT: access denied for usr1 to view 'your_schema.v1'")

        mdb.execute("GRANT SELECT on your_schema.v2 to usr1;").assertSucceeded()
        tc.execute("SELECT * FROM your_schema.v2;").assertSucceeded()\
            .assertDataResultMatch([('alice', '********A', Decimal('888.42')), ('bob', '********0', Decimal('444.42'))])
        tc.execute("SELECT * FROM your_schema.v1;")\
            .assertFailed(err_code="42000", err_message="SELECT: access denied for usr1 to view 'your_schema.v1'")

        mdb.execute("GRANT SELECT on your_schema.v1 to usr1;").assertSucceeded()
        tc.execute("SELECT * FROM your_schema.v1;").assertSucceeded()\
            .assertDataResultMatch([('alice', 1980, '********A', 'high'), ('bob', 1970, '********0', 'low')])
        tc.execute("SELECT * FROM your_schema.person;")\
            .assertFailed(err_code="42000", err_message="SELECT: access denied for usr1 to table 'your_schema.person'")
        tc.execute("SELECT * FROM your_schema.employee;")\
            .assertFailed(err_code="42000", err_message="SELECT: access denied for usr1 to table 'your_schema.employee'")

    # usr2 has access to one table and gets access to views top-down
    with SQLTestCase() as tc:
        tc.connect(username="usr2", password="p2")
        mdb.execute("GRANT SELECT on your_schema.employee to usr2;").assertSucceeded()

        mdb.execute("GRANT SELECT on your_schema.v4 to usr2;").assertSucceeded()
        tc.execute("SELECT * FROM your_schema.v4;").assertSucceeded()\
            .assertDataResultMatch([(1980, 'high'), (1970, 'low')])
        tc.execute("SELECT * FROM your_schema.v3;")\
            .assertFailed(err_code="42000", err_message="SELECT: access denied for usr2 to view 'your_schema.v3'")
        tc.execute("SELECT * FROM your_schema.v2;")\
            .assertFailed(err_code="42000", err_message="SELECT: access denied for usr2 to view 'your_schema.v2'")
        tc.execute("SELECT * FROM your_schema.v1;")\
            .assertFailed(err_code="42000", err_message="SELECT: access denied for usr2 to view 'your_schema.v1'")

        mdb.execute("GRANT SELECT on your_schema.v3 to usr2;").assertSucceeded()
        tc.execute("SELECT * FROM your_schema.v3;").assertSucceeded()\
            .assertDataResultMatch([('alice', 1980, Decimal('888.42')), ('bob', 1970, Decimal('444.42'))])
        tc.execute("SELECT * FROM your_schema.v2;")\
            .assertFailed(err_code="42000", err_message="SELECT: access denied for usr2 to view 'your_schema.v2'")
        tc.execute("SELECT * FROM your_schema.v1;")\
            .assertFailed(err_code="42000", err_message="SELECT: access denied for usr2 to view 'your_schema.v1'")

        mdb.execute("GRANT SELECT on your_schema.v2 to usr2;").assertSucceeded()
        tc.execute("SELECT * FROM your_schema.v2;").assertSucceeded()\
            .assertDataResultMatch([('alice', '********A', Decimal('888.42')), ('bob', '********0', Decimal('444.42'))])
        tc.execute("SELECT * FROM your_schema.v1;")\
            .assertFailed(err_code="42000", err_message="SELECT: access denied for usr2 to view 'your_schema.v1'")

        mdb.execute("GRANT SELECT on your_schema.v1 to usr2;").assertSucceeded()
        tc.execute("SELECT * FROM your_schema.v1;").assertSucceeded()\
            .assertDataResultMatch([('alice', 1980, '********A', 'high'), ('bob', 1970, '********0', 'low')])
        tc.execute("SELECT * FROM your_schema.person;")\
            .assertFailed(err_code="42000", err_message="SELECT: access denied for usr2 to table 'your_schema.person'")
        tc.execute("SELECT * FROM your_schema.employee;").assertRowCount(2)

    # clean up
    mdb.execute("DROP USER usr1;").assertSucceeded()
    mdb.execute("DROP USER usr2;").assertSucceeded()
    mdb.execute("DROP ROLE my_role;").assertSucceeded()
    mdb.execute("DROP SCHEMA my_schema CASCADE;").assertSucceeded()
    mdb.execute("DROP SCHEMA your_schema CASCADE;").assertSucceeded()

