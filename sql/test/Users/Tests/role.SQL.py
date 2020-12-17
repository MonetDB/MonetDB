###
# Tests for roles and users:
#   check that we cannot DROP an unexisting ROLE.
#   check that it is not possible to reCREATE an existing ROLE.
#   check that a USER can SET a GRANTed ROLE but cannot SET a non-GRANTed ROLE
#   check that we can DROP a ROLE after REVOKE
#   check that we cannot REVOKE a non-GRANTed ROLE
#   check that afer a ROLE is REVOKEd the USER can no longer assume it.
#   check that we cannot GRANT an unexisting ROLE.
###

from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    tc.connect(username="monetdb", password="monetdb")

    tc.execute("DROP ROLE non_existing_role;").assertFailed(err_code="0P000", err_message="DROP ROLE: no such role 'non_existing_role'")

    tc.execute("CREATE ROLE role1;").assertSucceeded()
    tc.execute("CREATE ROLE role2;").assertSucceeded()
    tc.execute("CREATE ROLE role3;").assertSucceeded()
    tc.execute("CREATE ROLE role1;").assertFailed(err_code="0P000", err_message="Role 'role1' already exists")

    tc.execute("CREATE USER alice with password 'alice' name 'alice' schema sys;")
    tc.execute("GRANT role1 TO alice;").assertSucceeded()
    tc.execute("GRANT role2 TO alice;").assertSucceeded()

    tc.connect(username="alice", password="alice")
    tc.execute("SET ROLE role1;").assertSucceeded()
    tc.execute("SET ROLE role3;").assertFailed(err_code="42000", err_message="Role (role3) missing")

    tc.connect(username="monetdb", password="monetdb")
    tc.execute("REVOKE role1 FROM alice;").assertSucceeded()
    tc.execute("REVOKE role2 FROM alice;").assertSucceeded()
    tc.execute("DROP ROLE role2;").assertSucceeded()

    tc.execute("REVOKE role3 FROM alice;").assertFailed(err_code="01006", err_message="REVOKE: User 'alice' does not have ROLE 'role3'")

    tc.connect(username="alice", password="alice")
    tc.execute("SET ROLE role1;").assertFailed(err_code="42000", err_message="Role (role1) missing")
    tc.execute("SET ROLE role2;").assertFailed(err_code="42000", err_message="Role (role2) missing")

    tc.connect(username="monetdb", password="monetdb")
    tc.execute("GRANT non_existing_role TO alice;").assertFailed(err_code="M1M05", err_message="GRANT: no such role 'non_existing_role' or grantee 'alice'")
    tc.execute("DROP ROLE role1;").assertSucceeded()
    tc.execute("DROP ROLE role3;").assertSucceeded()
    tc.execute("DROP USER alice;").assertSucceeded()
