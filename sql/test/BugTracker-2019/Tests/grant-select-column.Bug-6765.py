import sys, os, pymonetdb

db = os.getenv("TSTDB")
port = int(os.getenv("MAPIPORT"))

client1 = pymonetdb.connect(database=db, port=port, autocommit=True, username='monetdb', password='monetdb')
cur1 = client1.cursor()
cur1.execute('''
START TRANSACTION;
CREATE schema "myschema";
CREATE TABLE "myschema"."test" ("id" integer, "name" varchar(20));
INSERT INTO "myschema"."test" ("id", "name") VALUES (1,'Tom'),(2,'Karen');
CREATE USER myuser WITH UNENCRYPTED PASSWORD 'Test123' NAME 'Hulk' SCHEMA "myschema";
GRANT SELECT ON "myschema"."test" TO myuser;
COMMIT;
''')
cur1.execute('SELECT "name" FROM "myschema"."test";')
if cur1.fetchall() != [('Tom',), ('Karen',)]:
    sys.stderr.write('Expected result: [(\'Tom\',), (\'Karen\',)]')
cur1.close()
client1.close()

client1 = pymonetdb.connect(database=db, port=port, autocommit=True, username='myuser', password='Test123')
cur1 = client1.cursor()
cur1.execute('SELECT "id", "name" FROM "myschema"."test";')
if cur1.fetchall() != [(1,'Tom'),(2,'Karen')]:
    sys.stderr.write('Expected result: [(1,\'Tom\'),(2,\'Karen\')]')
cur1.close()
client1.close()

client1 = pymonetdb.connect(database=db, port=port, autocommit=True, username='monetdb', password='monetdb')
cur1 = client1.cursor()
cur1.execute('''
REVOKE SELECT ON "myschema"."test" FROM myuser;
GRANT SELECT ("name") ON "myschema"."test" TO myuser;
''')
cur1.close()
client1.close()

client1 = pymonetdb.connect(database=db, port=port, autocommit=True, username='myuser', password='Test123')
cur1 = client1.cursor()
try:
    cur1.execute('SELECT "id", "name" FROM "myschema"."test";')
    sys.stderr.write("Exception expected")
except pymonetdb.DatabaseError as e:
    if "identifier 'id' unknown" not in str(e):
        sys.stderr.write("Error: identifier 'id' unknown expected")
cur1.execute('SELECT "name" FROM "myschema"."test";')
if cur1.fetchall() != [('Tom',), ('Karen',)]:
    sys.stderr.write('Expected result: [(\'Tom\',), (\'Karen\',)]')
cur1.close()
client1.close()

client1 = pymonetdb.connect(database=db, port=port, autocommit=True, username='monetdb', password='monetdb')
cur1 = client1.cursor()
cur1.execute('''
DROP USER myuser;
DROP SCHEMA "myschema" CASCADE;
''')
cur1.close()
client1.close()
