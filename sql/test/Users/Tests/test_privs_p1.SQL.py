###
# SELECT, INSERT, UPDATE, DELETE a table for which the USER has GRANTs (possible).
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

nr=cursor.execute("select * from test")
if nr != 0:
    error("expected empty result")

rowaffected=cursor.execute("insert into test values(1,1)")
if rowaffected != 1:
    error("expected single insert")

rowaffected=cursor.execute("update test set b = 2")
if rowaffected != 1:
    error("expected single update")

rowaffected=cursor.execute("delete from test")
if rowaffected != 1:
    error("expected single delete")

cursor.close()
client.close()
