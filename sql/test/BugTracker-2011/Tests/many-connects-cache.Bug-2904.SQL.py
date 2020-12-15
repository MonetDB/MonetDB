import sys, os, pymonetdb, threading

db = os.getenv("TSTDB")
port = int(os.getenv("MAPIPORT"))

client = pymonetdb.connect(database=db, port=port, autocommit=True)
cursor = client.cursor()
cursor.execute("""
CREATE USER "testuser" WITH ENCRYPTED PASSWORD \'e9e633097ab9ceb3e48ec3f70ee2beba41d05d5420efee5da85f97d97005727587fda33ef4ff2322088f4c79e8133cc9cd9f3512f4d3a303cbdb5bc585415a00\' NAME \'Test User\' SCHEMA "sys";
CREATE SCHEMA "testschema" AUTHORIZATION "testuser";
ALTER USER "testuser" SET SCHEMA "testschema"
""")
cursor.close()
client.close()

client = pymonetdb.connect(database=db, port=port, autocommit=True, username='testuser', password='testpassword')
cursor = client.cursor()
cursor.execute('CREATE FUNCTION rad(d DOUBLE) RETURNS DOUBLE BEGIN RETURN d * PI() / 180; END')
cursor.close()
client.close()

class ConnectionClient(threading.Thread):
    def __init__(self):
        self._conn = pymonetdb.connect(database=db, port=port, autocommit=True, username='testuser', password='testpassword')
        self._cursor = self._conn.cursor()
        threading.Thread.__init__(self)

    def run(self):
        self._cursor.execute('SELECT rad(55.81689)')
        self._cursor.close()
        self._conn.close()

clients = []
for i in range(50):
    c = ConnectionClient()
    c.start()
    clients.append(c)

for c in clients:
    c.join()

client = pymonetdb.connect(database=db, port=port, autocommit=True, username='testuser', password='testpassword')
cursor = client.cursor()
cursor.execute('DROP FUNCTION rad')
cursor.close()
client.close()

client = pymonetdb.connect(database=db, autocommit=True, port=port)
cursor = client.cursor()
cursor.execute("""
ALTER USER "testuser" SET SCHEMA "sys";
DROP SCHEMA "testschema";
DROP USER "testuser"
""")
cursor.close()
client.close()
