import sys, os, pymonetdb

# Using schema path to find a function
db = os.getenv("TSTDB")
port = int(os.getenv("MAPIPORT"))

client1 = pymonetdb.connect(database=db, port=port, autocommit=True, username='monetdb', password='monetdb')
cur1 = client1.cursor()
cur1.execute('''
START TRANSACTION;
CREATE schema "sc1";
CREATE schema "sc2";
CREATE FUNCTION "sc1"."foo"() RETURNS INT BEGIN RETURN 1; END;
CREATE FUNCTION "sc2"."foo"() RETURNS INT BEGIN RETURN 2; END;
CREATE FUNCTION "sys"."foo"() RETURNS INT BEGIN RETURN 3; END;
CREATE USER myuser WITH UNENCRYPTED PASSWORD '1' NAME '1' SCHEMA "sys" SCHEMA PATH '"sc1","sc2"';
COMMIT;
''')
cur1.close()
client1.close()

client1 = pymonetdb.connect(database=db, port=port, autocommit=True, username='myuser', password='1')
cur1 = client1.cursor()
cur1.execute('SELECT "foo"();')
if cur1.fetchall() != [(3,)]:
    sys.stderr.write('Expected result: [(3,)]')
cur1.close()
client1.close()

client1 = pymonetdb.connect(database=db, port=port, autocommit=True, username='monetdb', password='monetdb')
cur1 = client1.cursor()
cur1.execute('''
START TRANSACTION;
ALTER USER myuser SCHEMA PATH '"sc1","sc2"'; -- no error expected
DROP FUNCTION "sys"."foo"();
COMMIT;
''')
cur1.close()
client1.close()

client1 = pymonetdb.connect(database=db, port=port, autocommit=True, username='myuser', password='1')
cur1 = client1.cursor()
cur1.execute('SELECT "foo"();')
if cur1.fetchall() != [(1,)]:
    sys.stderr.write('Expected result: [(1,)]')
cur1.close()
client1.close()

client1 = pymonetdb.connect(database=db, port=port, autocommit=True, username='monetdb', password='monetdb')
cur1 = client1.cursor()
cur1.execute('DROP FUNCTION "sc1"."foo"();')
cur1.close()
client1.close()

client1 = pymonetdb.connect(database=db, port=port, autocommit=True, username='myuser', password='1')
cur1 = client1.cursor()
cur1.execute('SELECT "foo"();')
if cur1.fetchall() != [(2,)]:
    sys.stderr.write('Expected result: [(2,)]')
cur1.close()
client1.close()

client1 = pymonetdb.connect(database=db, port=port, autocommit=True, username='monetdb', password='monetdb')
cur1 = client1.cursor()
try:
    cur1.execute('ALTER USER myuser SCHEMA PATH \'sc1\';')
    sys.stderr.write("Exception expected")
except pymonetdb.DatabaseError as e:
    if "A schema in the path must be within '\"'" not in str(e):
        sys.stderr.write("Error: \"A schema in the path must be within '\"'\" expected")
try:
    cur1.execute('ALTER USER myuser SCHEMA PATH \'"sc1\';')
    sys.stderr.write("Exception expected")
except pymonetdb.DatabaseError as e:
    if " A schema path cannot end inside inside a schema name" not in str(e):
        sys.stderr.write("Error: \"A schema path cannot end inside inside a schema name\" expected")
try:
    cur1.execute('ALTER USER myuser SCHEMA PATH \'"sc1\';')
    sys.stderr.write("Exception expected")
except pymonetdb.DatabaseError as e:
    if " A schema path cannot end inside inside a schema name" not in str(e):
        sys.stderr.write("Error: \"A schema path cannot end inside inside a schema name\" expected")

cur1.execute('''
START TRANSACTION;
DROP USER myuser;
DROP schema "sc1" CASCADE;
DROP schema "sc2" CASCADE;
COMMIT;
''')
cur1.close()
client1.close()
