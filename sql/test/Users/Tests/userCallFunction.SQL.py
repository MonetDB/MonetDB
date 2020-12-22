###
# Check that a user can only execute a function after the user has been granted
#   the EXECUTE rights.
# Also check that function signature matters.
###

from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as mdb:
    mdb.connect(username="monetdb", password="monetdb")
    mdb.execute("""
        start transaction;
        create schema s1;
        CREATE USER u1 WITH PASSWORD '1' NAME 'u1' SCHEMA s1;
        CREATE FUNCTION s1.f1(a int) RETURNS INT BEGIN RETURN 10 + a; END;
        CREATE FUNCTION s1.f1() RETURNS INT BEGIN RETURN 10; END;
        commit;
    """).assertSucceeded()

    with SQLTestCase() as tc:
        tc.connect(username="u1", password="1")
        tc.execute('SELECT s1.f1();').assertFailed(err_code="42000", err_message="SELECT: no such operator 's1'.'f1'()")
        tc.execute('SELECT s1.f1(1);').assertFailed(err_code="42000", err_message="SELECT: no such unary operator 's1'.'f1'(tinyint)")
        tc.execute('SELECT s1.f1(cast(1 as int));').assertFailed(err_code="42000", err_message="SELECT: no such unary operator 's1'.'f1'(int)")
        tc.execute('CALL sys.flush_log();').assertFailed(err_code="42000", err_message="SELECT: no such operator 'sys'.'flush_log'()")

        mdb.execute('GRANT EXECUTE ON FUNCTION s1.f1 TO u1;').assertFailed(err_code="42000", err_message="GRANT FUNCTION: there are more than one function called 'f1', please use the full signature")

        mdb.execute('GRANT EXECUTE ON FUNCTION s1.f1() TO u1;').assertSucceeded()
        tc.execute('SELECT s1.f1();').assertDataResultMatch([(10,)])
        tc.execute('SELECT s1.f1(1);').assertFailed(err_code="42000", err_message="SELECT: no such unary operator 's1'.'f1'(tinyint)")
        tc.execute('SELECT s1.f1(cast(1 as int));').assertFailed(err_code="42000", err_message="SELECT: no such unary operator 's1'.'f1'(int)")

        mdb.execute('REVOKE EXECUTE ON FUNCTION s1.f1() FROM u1;').assertSucceeded()
        tc.execute('SELECT s1.f1();').assertFailed(err_code="42000", err_message="SELECT: no such operator 's1'.'f1'()")
        mdb.execute('GRANT EXECUTE ON FUNCTION s1.f1(int) TO u1;').assertSucceeded()
        tc.execute('SELECT s1.f1(1);').assertDataResultMatch([(11,)])
        tc.execute('SELECT s1.f1(cast(1 as int));').assertDataResultMatch([(11,)])

    mdb.execute("""
        start transaction;
        drop user u1;
        drop schema s1 cascade;
        commit;
    """)
