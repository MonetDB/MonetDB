###
# Check that GRANT a ROLE to a USER once works, but GRANT it a second time is
#   properly rejected with "GRANT: User '<usr>' already has ROLE '<role>'",
#   which also prevents the related problems described in this bug, i.e.
#   duplicate entries are created in sys.user_role and subsequent REVOKE
#   doesn't work.
###

from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as mdb:
    mdb.connect(username="monetdb", password="monetdb")
    mdb.execute("create user mydummyuser with password 'mydummyuser' name 'mydummyuser' schema sys;").assertSucceeded()

    with SQLTestCase() as tc:
        tc.connect(username="mydummyuser", password="mydummyuser")
        tc.execute("set role sysadmin;").assertFailed(err_code="42000", err_message="Role (sysadmin) missing")

        mdb.execute("select count(*) from user_role where login_id in (select id from sys.auths where name = 'mydummyuser');").assertDataResultMatch([(0,)])
        mdb.execute("grant sysadmin to mydummyuser;").assertSucceeded()
        mdb.execute("select count(*) from user_role where login_id in (select id from sys.auths where name = 'mydummyuser');").assertDataResultMatch([(1,)])

        tc.execute("set role sysadmin;").assertSucceeded()

        mdb.execute("grant sysadmin to mydummyuser;").assertFailed(err_code="M1M05", err_message="GRANT: User 'mydummyuser' already has ROLE 'sysadmin'")
        mdb.execute("select count(*) from user_role where login_id in (select id from sys.auths where name = 'mydummyuser');").assertDataResultMatch([(1,)])

        mdb.execute("revoke sysadmin from mydummyuser;").assertSucceeded()
        mdb.execute("select count(*) from user_role where login_id in (select id from sys.auths where name = 'mydummyuser');").assertDataResultMatch([(0,)])

        mdb.execute("revoke sysadmin from mydummyuser;").assertFailed(err_code="01006", err_message="REVOKE: User 'mydummyuser' does not have ROLE 'sysadmin'")
        mdb.execute("select count(*) from user_role where login_id in (select id from sys.auths where name = 'mydummyuser');").assertDataResultMatch([(0,)])

    # clean up
    mdb.execute("drop user mydummyuser;").assertSucceeded()
