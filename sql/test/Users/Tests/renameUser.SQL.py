###
# Let admin rename a user (from A to B) (possible).
# Create another user with the old name (A)(possible).
# Verify that the new user (A) cannot make use of the role assign to the inital user (now B).
# Verify that a user with no special permissions cannot rename users.
# Verify that the renamed user (B) still has his rights.
# Rename a user with an already existing name (not possible).
# Rename an unexisting user (not possible).
# Create an user on a non-existing schema (not possible).
# Create an user with a name of an existing user (not possible).
###

from util import sql_test_client

sql_test_client('monetdb', 'monetdb', input = """\
ALTER USER "april" RENAME TO "april2"; --succeed
CREATE USER april with password 'april' name 'second' schema bank;
""")

# This is the new april, so these operations should fail.
sql_test_client('april', 'april', input = """\
DELETE from bank.accounts; -- not enough privelges
SET role bankAdmin; -- no such role
ALTER USER "april2" RENAME TO "april3"; --not enough privileges
""")

# This is the initial april, so these operations should succeed.
sql_test_client('april2', 'april', input = """\
SELECT * from bank.accounts;
SET role bankAdmin;
""")

sql_test_client('monetdb', 'monetdb', input = """\
ALTER USER "april2" RENAME TO "april";
drop user april;
ALTER USER "april2" RENAME TO "april";
ALTER USER "april5" RENAME TO "april2"; -- no such user
drop user april2; --nu such user
CREATE USER april2 with password 'april' name 'second april, no rights' schema library2; --no such schema
CREATE USER april with password 'april' name 'second april, no rights' schema library; --user exsists
""")
