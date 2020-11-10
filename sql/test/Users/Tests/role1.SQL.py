###
# SET a GRANTed ROLE for a USER (possible).
###

import os, sys
import pymonetdb


db=os.getenv("TSTDB")
port=int(os.getenv("MAPIPORT"))
client = pymonetdb.connect(database=db, port=port, autocommit=True, user='my_user', password='p1')
cursor = client.cursor()

# exceptions will output
cursor.execute("SET ROLE my_role")

cursor.close()
client.close()
