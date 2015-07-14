###
# Give a privilege to a USER, then remove it and give it again (possible).
# Assess that by granting one privilege, he only gets that one privilege.
# Assess that the privilege was indeed removed.
# Assess that it is possible to regrant the same privilege.
###

import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

def client(user, passwd, input=None):
    clt = process.client(lang='sql', user=user, passwd=passwd,
                         stdin = process.PIPE,
                         stdout = process.PIPE,
                         stderr = process.PIPE,
                         port = int(os.getenv('MAPIPORT')))
    out, err = clt.communicate(input)
    sys.stdout.write(out)
    sys.stderr.write(err)

sql_client = os.getenv('SQL_CLIENT')


client('monetdb', 'monetdb', input = """\
CREATE SCHEMA schemaTest;

CREATE USER user_delete with password 'delete' name 'user can only delete' schema schemaTest;
CREATE USER user_insert with password 'insert' name 'user can only insert' schema schemaTest;
CREATE USER user_update with password 'update' name 'user can only update' schema schemaTest;
CREATE USER user_select with password 'select' name 'user can only select' schema schemaTest;

CREATE table schemaTest.testTable (v1 int, v2 int);

INSERT into schemaTest.testTable values(1, 1);
INSERT into schemaTest.testTable values(2, 2);
INSERT into schemaTest.testTable values(3, 3);

-- Grant delete rights.
GRANT DELETE on table schemaTest.testTable to user_delete;
GRANT INSERT on table schemaTest.testTable to user_insert;
GRANT UPDATE on table schemaTest.testTable to user_update;
GRANT SELECT on table schemaTest.testTable to user_select;

""")

client('user_delete', 'delete', input = """\
-- Check delete.
DELETE FROM testTable where v1 = 2;

-- Check all the other privileges (they should fail).
SELECT * FROM testTable;
UPDATE testTable set v1 = 2 where v2 = 7;
INSERT into testTable values (3, 3);
""")

client('user_update', 'update', input = """\
-- Check insert.
UPDATE testTable set v1 = 2 where v2 = 7;

-- Check all the other privileges (they should fail).
SELECT * FROM testTable;
INSERT into testTable values (3, 3);
DELETE FROM testTable where v1 = 2;
""")

client('user_insert', 'insert', input = """\
-- Check insert.
INSERT into testTable values (3, 3);

-- Check all the other privileges (they should fail).
SELECT * FROM testTable;
UPDATE testTable set v1 = 2 where v2 = 7;
DELETE FROM testTable where v1 = 2;
""")

client('user_select', 'select', input = """\
-- Check insert.
SELECT * FROM testTable;

-- Check all the other privileges (they should fail).
INSERT into testTable values (3, 3);
UPDATE testTable set v1 = 2 where v2 = 7;
DELETE FROM testTable where v1 = 2;
""")

client('monetdb', 'monetdb', input = """\
SELECT * FROM schemaTest.testTable;

REVOKE DELETE on schemaTest.testTable from user_delete;
REVOKE INSERT on schemaTest.testTable from user_insert;
REVOKE UPDATE on schemaTest.testTable from user_update;
REVOKE SELECT on schemaTest.testTable from user_select;
""")

# Next four transitions should not be allowed.
client('user_delete', 'delete', input = """\
DELETE from testTable where v2 = 666;
""")

client('user_insert', 'insert', input = """\
INSERT into testTable values (666, 666);
""")

client('user_update', 'update', input = """\
UPDATE testTable set v1 = 666 where v2 = 666;
""")

client('user_select', 'select', input = """\
SELECT * FROM testTable where v1 = 666;
""")
#

# Regrant the revoked permissions to the users.
client('monetdb', 'monetdb', input = """\
SELECT * from schemaTest.testTable;

-- Grant delete rights.
GRANT DELETE on table schemaTest.testTable to user_delete;
GRANT INSERT on table schemaTest.testTable to user_insert;
GRANT UPDATE on table schemaTest.testTable to user_update;
GRANT SELECT on table schemaTest.testTable to user_select;
""")

# Next four transitions should be allowed.
client('user_delete', 'delete', input = """\
DELETE from testTable where v1 = 42;
""")

client('user_insert', 'insert', input = """\
INSERT into testTable values (42, 42);
""")

client('user_update', 'update', input = """\
UPDATE testTable set v1 = 42 where v2 = 42;
""")

client('user_select', 'select', input = """\
SELECT * FROM testTable where v1 = 42;
""")

# XXX: Clean up?



