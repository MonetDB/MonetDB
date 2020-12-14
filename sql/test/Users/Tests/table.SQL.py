###
# SET a GRANTed ROLE for a USER (possible).
# CREATE TABLE and INSERT (possible).
###
from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    tc.connect(username="my_user", password="p1")
    tc.execute("SET ROLE my_role").assertSucceeded()
    tc.execute("""
    CREATE TABLE my_schema.my_table (
      obsid INT NOT NULL AUTO_INCREMENT,
      time_s BIGINT NULL,
      time_e BIGINT NULL,
      PRIMARY KEY (obsid)
    ) """).assertSucceeded()
    tc.execute("INSERT INTO my_schema.my_table (time_s) values (300)")\
            .assertSucceeded()\
            .assertRowCount(1)

# import os, sys
# import pymonetdb

# db=os.getenv("TSTDB")
# port=int(os.getenv("MAPIPORT"))
# client = pymonetdb.connect(database=db, port=port, autocommit=True, user='my_user', password='p1')
# cursor = client.cursor()

# # exceptions will output
# cursor.execute("SET ROLE my_role")
# cursor.execute("""
# CREATE TABLE my_schema.my_table (
#   obsid INT NOT NULL AUTO_INCREMENT,
#   time_s BIGINT NULL,
#   time_e BIGINT NULL,
#   PRIMARY KEY (obsid)
# ) """)
# rowsaffected=cursor.execute("INSERT INTO my_schema.my_table (time_s) values (300)")
# if rowsaffected != 1:
#     print("affected rows should be 1, not %d\n" % rwosaffected)
#     sys.exit(-1)

# cursor.close()
# client.close()
