###
# SELECT, INSERT, UPDATE, DELETE a table for which the USER does *not* have GRANTs (not possible).
###

import os, sys
import pymonetdb


db=os.getenv("TSTDB")
port=int(os.getenv("MAPIPORT"))
client = pymonetdb.connect(database=db, port=port, autocommit=True, user='my_user2', password='p2')
cursor = client.cursor()

err=0
try:
    nr=cursor.execute("select * from test")
except:
    err=1
    pass

try:
    rowaffected=cursor.execute("insert into test values(1,1)")
except:
    err=err+2
    pass

try:
    rowaffected=cursor.execute("update test set b = 2")
except:
    err=err+4
    pass

try:
    rowaffected=cursor.execute("delete from test")
except:
    err=err+8
    pass

if err != 15:
    print("User should have no access too this table\n", file=sys.stderr)
    sys.exit(-1)

cursor.close()
client.close()
