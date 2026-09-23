from MonetDBtesting import tpymonetdb as pymonetdb
import sys,os

db = os.getenv("TSTDB")
port = int(os.getenv("MAPIPORT"))

connections = []
for i in range(130):
    conn = pymonetdb.connect(database=db, port=port, autocommit=True, username='monetdb', password='monetdb')
    connections.append(conn)

for conn in connections:
    conn.close()

conn = pymonetdb.connect(database=db, port=port, autocommit=True, username='monetdb', password='monetdb')
conn.close()
