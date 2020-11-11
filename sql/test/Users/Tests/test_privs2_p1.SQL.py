###
# Use functions that contain SELECT. INSERT, UPDATE, DELETE
# on a table for which the USER has GRANTs (possible).
###

import os, sys
import pymonetdb

db=os.getenv("TSTDB")
port=int(os.getenv("MAPIPORT"))
client = pymonetdb.connect(database=db, port=port, autocommit=True, user='my_user', password='p1')
cursor = client.cursor()

def error(msg):
    print(msg)
    sys.exit(-1)

nr=cursor.execute("SELECT * FROM version")
if nr != 1:
    error("expected single row result from version")
rows=cursor.fetchall()
if rows[0][0] != 'test1':
    error("expected first row with 'test1'")

rowaffected=cursor.execute("insert into version (name, i) values ('test2', 2)")
if rowaffected != 1:
    error("expected single insert")

nr=cursor.execute("SELECT insertversion('test3', 3)")
if nr != 1:
    error("expected single row result from insertversion")
rows=cursor.fetchall()
if rows[0][0] != 1:
    error("expected first row with '1' not '%d'" % rows[0][0])

nr=cursor.execute("SELECT * FROM version")
if nr != 3:
    error("expected 3 rows")
rows=cursor.fetchall()
if rows[2][0] != 'test3':
    error("expected last row with 'test3'")

nr=cursor.execute("SELECT updateversion('test1', 4)")
if nr != 1:
    error("expected single row result from updateversion")
rows=cursor.fetchall()
if rows[0][0] != 1:
    error("expected first row with '1' not '%d'" % rows[0][0])

nr=cursor.execute("SELECT * FROM version")
if nr != 3:
    error("expected 3 rows")
rows=cursor.fetchall()
if rows[0][1] != 4:
    error("expected first row with updated value '4', not '%d'" % row[0][1])

nr=cursor.execute("SELECT deleteversion('test1')")
if nr != 1:
    error("expected single row result from deleteversion")
rows=cursor.fetchall()
if rows[0][0] != 1:
    error("expected first row with '1' not '%d'" % rows[0][0])

nr=cursor.execute("SELECT * FROM version")
if nr != 2:
    error("expected 2 rows")
rows=cursor.fetchall()
if rows[0][0] != 'test2':
    error("expected first row after delete to be 'test2'")

cursor.close()
client.close()
