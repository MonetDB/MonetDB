###
# Use functions that contain SELECT. INSERT, UPDATE, DELETE on a
# table for which the USER does *not* have GRANTs (not possible).
###

from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    tc.connect(username="my_user2", password="p2")
    tc.execute("SELECT * FROM version").assertFailed()
    tc.execute("insert into version (name ,i) values ('test2' ,2)").assertFailed()
    tc.execute("SELECT insertversion('test3', 3)").assertFailed()
    tc.execute("SELECT updateversion('test1', 4)").assertFailed()
    tc.execute("SELECT deleteversion('test1')").assertFailed()

# import os, sys
# import pymonetdb


# db=os.getenv("TSTDB")
# port=int(os.getenv("MAPIPORT"))
# client = pymonetdb.connect(database=db, port=port, autocommit=True, user='my_user2', password='p2')
# cursor = client.cursor()

# def sel1():
#     err=0
#     try:
#         cursor.execute("SELECT * FROM version")
#     except:
#         err=1
#         pass
#     return err

# err = sel1()

# try:
#     cursor.execute("insert into version (name ,i) values ('test2' ,2)")
# except:
#     err=err+2
#     pass

# sel1()

# try:
#     cursor.execute("SELECT insertversion('test3', 3)")
# except:
#     err=err+4
#     pass

# sel1()

# try:
#     cursor.execute("SELECT updateversion('test1', 4)")
# except:
#     err=err+8
#     pass

# sel1()

# try:
#     cursor.execute("SELECT deleteversion('test1')")
# except:
#     err=err+16
#     pass

# sel1()

# if err != 31:
#     print("User should have no access too this table\n")
#     sys.exit(-1)

# cursor.close()
# client.close()
