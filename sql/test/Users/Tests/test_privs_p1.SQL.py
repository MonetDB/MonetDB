###
# SELECT, INSERT, UPDATE, DELETE a table for which the USER has GRANTs (possible).
###

from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    tc.connect(username="my_user", password="p1")
    tc.execute("select * from test").assertSucceeded().assertRowCount(0)
    tc.execute("insert into test values(1,1)").assertSucceeded().assertRowCount(1)
    tc.execute("update test set b = 2").assertSucceeded().assertRowCount(1)
    tc.execute("delete from test").assertSucceeded().assertRowCount(1)

# import os, sys
# import pymonetdb

# db=os.getenv("TSTDB")
# port=int(os.getenv("MAPIPORT"))
# client = pymonetdb.connect(database=db, port=port, autocommit=True, user='my_user', password='p1')
# cursor = client.cursor()

# def error(msg):
#     print(msg)
#     sys.exit(-1)

# nr=cursor.execute("select * from test")
# if nr != 0:
#     error("expected empty result")

# rowaffected=cursor.execute("insert into test values(1,1)")
# if rowaffected != 1:
#     error("expected single insert")

# rowaffected=cursor.execute("update test set b = 2")
# if rowaffected != 1:
#     error("expected single update")

# rowaffected=cursor.execute("delete from test")
# if rowaffected != 1:
#     error("expected single delete")

# cursor.close()
# client.close()
