import sys, os, pymonetdb

db = os.getenv("TSTDB")
port = int(os.getenv("MAPIPORT"))

client = pymonetdb.connect(database=db, port=port, autocommit=True)
cursor = client.cursor()
cursor.execute('CREATE USER t WITH PASSWORD \'1\' NAME \'t\' SCHEMA "sys"')
cursor.close()
client.close()

client = pymonetdb.connect(database=db, port=port, autocommit=True, username='t', password='1')
cursor = client.cursor()
try:
    cursor.execute('CREATE GLOBAL TEMPORARY TABLE TempTable (i int)')
    sys.stderr.write('Exception expected')
except Exception as ex:
    if 'insufficient privileges for user \'t\' in schema \'tmp\'' not in ex.__str__():
        sys.stderr.write(ex.__str__())
cursor.close()
client.close()

client = pymonetdb.connect(database=db, port=port, autocommit=True, username='t', password='1')
cursor = client.cursor()
cursor.execute('CREATE LOCAL TEMPORARY TABLE TempTable (i int)')
cursor.close()
client.close()

client = pymonetdb.connect(database=db, autocommit=True, port=port)
cursor = client.cursor()
cursor.execute('DROP USER t')
cursor.close()
client.close()
