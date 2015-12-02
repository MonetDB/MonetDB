###
# Give a privilege to a USER, then remove it and give it again (possible).
# Assess that by granting one privilege, he only gets that one privilege.
# Assess that the privilege was indeed removed.
# Assess that it is possible to regrant the revoked privilege.
###

from util import sql_test_client

sql_test_client('monetdb', 'monetdb', input = """\
CREATE SCHEMA schemaTest;

CREATE USER user_delete with password 'delete' name 'user can only delete' schema schemaTest;
CREATE USER user_insert with password 'insert' name 'user can only insert' schema schemaTest;
CREATE USER user_update with password 'update' name 'user can only update' schema schemaTest;
CREATE USER user_select with password 'select' name 'user can only select' schema schemaTest;

CREATE table schemaTest.testTable (v1 int, v2 int);

INSERT into schemaTest.testTable values(1, 1);
INSERT into schemaTest.testTable values(2, 2);
INSERT into schemaTest.testTable values(3, 3);

-- Grant rights.
GRANT DELETE on table schemaTest.testTable to user_delete;
GRANT INSERT on table schemaTest.testTable to user_insert;
GRANT UPDATE on table schemaTest.testTable to user_update;
GRANT SELECT on table schemaTest.testTable to user_delete;
GRANT SELECT on table schemaTest.testTable to user_update;
GRANT SELECT on table schemaTest.testTable to user_select;

""")

sql_test_client('user_delete', 'delete', input = """\
DELETE FROM testTable where v1 = 2; -- should work

-- Check all the other privileges (they should fail).
SELECT * FROM testTable; -- not enough privileges
UPDATE testTable set v1 = 2 where v2 = 7; -- not enough privileges
INSERT into testTable values (3, 3); -- not enough privileges
""")

sql_test_client('user_update', 'update', input = """\
-- Check insert.
UPDATE testTable set v1 = 2 where v2 = 7;

-- Check all the other privileges (they should fail).
SELECT * FROM testTable; -- not enough privileges
INSERT into testTable values (3, 3); -- not enough privileges
DELETE FROM testTable where v1 = 2; -- not enough privileges
""")

sql_test_client('user_insert', 'insert', input = """\
-- Check insert.
INSERT into testTable values (3, 3);

-- Check all the other privileges (they should fail).
SELECT * FROM testTable; -- not enough privileges
UPDATE testTable set v1 = 2 where v2 = 7; -- not enough privileges
DELETE FROM testTable where v1 = 2; -- not enough privileges
""")

sql_test_client('user_select', 'select', input = """\
-- Check insert.
SELECT * FROM testTable;

-- Check all the other privileges (they should fail).
INSERT into testTable values (3, 3); -- not enough privileges
UPDATE testTable set v1 = 2 where v2 = 7; -- not enough privileges
DELETE FROM testTable where v1 = 2; -- not enough privileges
""")

sql_test_client('monetdb', 'monetdb', input = """\
SELECT * FROM schemaTest.testTable;

REVOKE DELETE on schemaTest.testTable from user_delete;
REVOKE INSERT on schemaTest.testTable from user_insert;
REVOKE UPDATE on schemaTest.testTable from user_update;
REVOKE SELECT on schemaTest.testTable from user_select;
""")

# Next four transitions should not be allowed.
sql_test_client('user_delete', 'delete', input = """\
DELETE from testTable where v2 = 666; -- not enough privileges
""")

sql_test_client('user_insert', 'insert', input = """\
INSERT into testTable values (666, 666); -- not enough privileges
""")

sql_test_client('user_update', 'update', input = """\
UPDATE testTable set v1 = 666 where v2 = 666; -- not enough privileges
""")

sql_test_client('user_select', 'select', input = """\
SELECT * FROM testTable where v1 = 666; -- not enough privileges
""")
#

# Regrant the revoked permissions to the users.
sql_test_client('monetdb', 'monetdb', input = """\
SELECT * from schemaTest.testTable;

-- Grant delete rights.
GRANT DELETE on table schemaTest.testTable to user_delete;
GRANT INSERT on table schemaTest.testTable to user_insert;
GRANT UPDATE on table schemaTest.testTable to user_update;
GRANT SELECT on table schemaTest.testTable to user_select;
""")

# Next four transitions should be allowed.
sql_test_client('user_delete', 'delete', input = """\
DELETE from testTable where v1 = 42; -- privilege granted
""")

sql_test_client('user_insert', 'insert', input = """\
INSERT into testTable values (42, 42); -- privilege granted
""")

sql_test_client('user_update', 'update', input = """\
UPDATE testTable set v1 = 42 where v2 = 42; -- privilege granted
""")

sql_test_client('user_select', 'select', input = """\
SELECT * FROM testTable where v1 = 42; -- privilege granted
""")
