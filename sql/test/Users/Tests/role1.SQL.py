###
# SET a GRANTed ROLE for a USER (possible).
# SET a non-GRANTed ROLE for a USER (not possible).
###

import os, sys
import pymonetdb


db=os.getenv("TSTDB")
port=int(os.getenv("MAPIPORT"))
mdbconn = pymonetdb.connect(database=db, port=port, autocommit=True, user='monetdb', password='monetdb')
mdb = mdbconn.cursor()
mdb.execute("CREATE ROLE my_role")
mdb.execute("CREATE SCHEMA my_schema AUTHORIZATION my_role")
mdb.execute("CREATE USER my_user with password 'p1' name 'User with role' schema my_schema")
mdb.execute("GRANT my_role to my_user")
mdb.execute("CREATE USER my_user2 with password 'p2' name 'User without role' schema my_schema")

client = pymonetdb.connect(database=db, port=port, autocommit=True, user='my_user', password='p1')
cursor = client.cursor()
# exceptions will output
cursor.execute("SET ROLE my_role")
cursor.close()
client.close()

client = pymonetdb.connect(database=db, port=port, autocommit=True, user='my_user2', password='p2')
cursor = client.cursor()
# exception "!Role (my_role) missing" is expected
try:
    cursor.execute("SET ROLE my_role")
except:
    pass
cursor.close()
client.close()

mdb.execute("DROP USER my_user")
mdb.execute("DROP USER my_user2")
mdb.execute("DROP ROLE my_role")
mdb.execute("DROP SCHEMA my_schema")
mdb.close()
mdbconn.close()
