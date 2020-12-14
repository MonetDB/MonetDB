import sys, os, pymonetdb

db = os.getenv("TSTDB")
port = int(os.getenv("MAPIPORT"))

client1 = pymonetdb.connect(database=db, port=port, autocommit=True, username='monetdb', password='monetdb')
cur1 = client1.cursor()
cur1.execute('create user "mydummyuser" with password \'mydummyuser\' name \'mydummyuser\' schema "sys";')
cur1.close()
client1.close()

client1 = pymonetdb.connect(database=db, port=port, autocommit=True, username='mydummyuser', password='mydummyuser')
cur1 = client1.cursor()
try:
    cur1.execute('set role "sysadmin"; --error')
    sys.stderr.write("Exception expected")
except pymonetdb.DatabaseError as e:
    if "Role (sysadmin) missing" not in str(e):
        sys.stderr.write("Error: Role (sysadmin) missing")
cur1.close()
client1.close()

client1 = pymonetdb.connect(database=db, port=port, autocommit=True, username='monetdb', password='monetdb')
cur1 = client1.cursor()
cur1.execute('select count(*) from "user_role" where "login_id" in (select "id" from "sys"."auths" where "name" = \'mydummyuser\');')
if cur1.fetchall() != [(0,)]:
    sys.stderr.write('Expected result: [(0,)]')
cur1.execute('grant "sysadmin" to "mydummyuser";')
cur1.close()
client1.close()

client1 = pymonetdb.connect(database=db, port=port, autocommit=True, username='mydummyuser', password='mydummyuser')
cur1 = client1.cursor()
cur1.execute('set role "sysadmin";')
cur1.close()
client1.close()

client1 = pymonetdb.connect(database=db, port=port, autocommit=True, username='monetdb', password='monetdb')
cur1 = client1.cursor()
cur1.execute('select count(*) from "user_role" where "login_id" in (select "id" from "sys"."auths" where "name" = \'mydummyuser\');')
if cur1.fetchall() != [(1,)]:
    sys.stderr.write('Expected result: [(1,)]')
try:
    cur1.execute('grant "sysadmin" to "mydummyuser"; --error')
    sys.stderr.write("Exception expected")
except pymonetdb.DatabaseError as e:
    if "User 'mydummyuser' already has ROLE 'sysadmin'" not in str(e):
        sys.stderr.write("Error: User 'mydummyuser' already has ROLE 'sysadmin'")
cur1.execute('select count(*) from "user_role" where "login_id" in (select "id" from "sys"."auths" where "name" = \'mydummyuser\');')
if cur1.fetchall() != [(1,)]:
    sys.stderr.write('Expected result: [(1,)]')
cur1.execute('revoke "sysadmin" from "mydummyuser";')
cur1.execute('select count(*) from "user_role" where "login_id" in (select "id" from "sys"."auths" where "name" = \'mydummyuser\');')
if cur1.fetchall() != [(0,)]:
    sys.stderr.write('Expected result: [(0,)]')
try:
    cur1.execute('revoke "sysadmin" from "mydummyuser"; --error')
    sys.stderr.write("Exception expected")
except pymonetdb.DatabaseError as e:
    if "User 'mydummyuser' does not have ROLE 'sysadmin'" not in str(e):
        sys.stderr.write("Error: User 'mydummyuser' does not have ROLE 'sysadmin'")
cur1.execute('select count(*) from "user_role" where "login_id" in (select "id" from "sys"."auths" where "name" = \'mydummyuser\');')
if cur1.fetchall() != [(0,)]:
    sys.stderr.write('Expected result: [(0,)]')
cur1.execute('drop user "mydummyuser";')
cur1.close()
client1.close()
