###
# Check that a user cannot grant a role to anyone, including themselves, if the
#   role was not assigned to this user.
# Check that the monetdb user, the role owner or a user granted with sysadmin
#   can grant roles
###

from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as mdb:
    mdb.connect(username="monetdb", password="monetdb")
    mdb.execute("""
        CREATE SCHEMA s1;
        CREATE USER bruce WITH PASSWORD 'bruce' name 'willis' schema s1;
        CREATE USER alice WITH PASSWORD 'alice' name 'wonderland' schema s1;
        CREATE TABLE s1.test(d int);
        CREATE ROLE role1;
        GRANT ALL ON s1.test to role1;
     """).assertSucceeded()

    with SQLTestCase() as bruce:
        # check that bruce cannot grant role1
        bruce.connect(username="bruce", password="bruce")
        bruce.execute("SET role role1;").assertFailed(err_code="42000", err_message="Role (role1) missing")
        bruce.execute("select * from test;").assertFailed(err_code="42000", err_message="SELECT: access denied for bruce to table 's1.test'")
        bruce.execute("GRANT role1 TO alice FROM current_user;").assertFailed(err_code="0P000", err_message="GRANT: Insufficient privileges to grant ROLE 'role1'")
        bruce.execute("GRANT role1 TO bruce FROM current_user;").assertFailed(err_code="0P000", err_message="GRANT: Insufficient privileges to grant ROLE 'role1'")

        # check that bruce can grant role1 once he's assigned the role
        mdb.execute("GRANT role1 TO bruce WITH ADMIN OPTION;").assertSucceeded()
        bruce.execute("SET role role1;").assertSucceeded()
        bruce.execute("select * from test;").assertSucceeded()
        bruce.execute("GRANT role1 TO alice FROM current_user;").assertSucceeded()
        bruce.execute("GRANT role1 TO bruce FROM current_user;").assertFailed(err_code="M1M05", err_message="GRANT: User 'bruce' already has ROLE 'role1'")

        # revoke role1 from bruce and check that he's lost his privileges
        mdb.execute("REVOKE role1 FROM bruce;").assertSucceeded()
        mdb.execute("REVOKE role1 FROM alice;").assertSucceeded()
        with SQLTestCase() as bruce1: # reconnect to make sure role is gone.
            bruce1.connect(username="bruce", password="bruce")
            bruce1.execute("select * from test;").assertFailed(err_code="42000", err_message="SELECT: access denied for bruce to table 's1.test'")
            bruce1.execute("GRANT role1 TO alice FROM current_user;").assertFailed(err_code="0P000", err_message="GRANT: Insufficient privileges to grant ROLE 'role1'")

            # grant sysadmin to bruce and check that he now can do everything again
            # as a sysadmin
            mdb.execute("GRANT sysadmin TO bruce;").assertSucceeded()
            bruce1.execute("SET role sysadmin;").assertSucceeded()
            bruce1.execute("select * from test;").assertSucceeded()
            bruce1.execute("GRANT role1 TO alice FROM current_role;").assertSucceeded()

