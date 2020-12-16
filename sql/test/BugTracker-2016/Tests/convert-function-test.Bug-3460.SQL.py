from MonetDBtesting.sqltest import SQLTestCase
import pymonetdb, os

conn1 = pymonetdb.connect(database=os.getenv("TSTDB"), port=int(os.getenv("MAPIPORT")), autocommit=True)
cur1 = conn1.cursor()
try:
    cur1.execute('select cast(1 as hugeint)')
    suffix = '.int128'
except pymonetdb.DatabaseError as e:
    suffix = ''
cur1.close()
conn1.close()

with SQLTestCase() as tc:
    tc.connect()
    with open('convert-function-test.Bug-3460.sql') as f:
        tc.execute(query=None, client='mclient', stdin=f)\
            .assertMatchStableOut(fout='convert-function-test.Bug-3460.stable.out%s' % (suffix))\
            .assertMatchStableError(ferr='convert-function-test.Bug-3460.stable.err%s' % (suffix))
