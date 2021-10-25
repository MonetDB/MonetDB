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
    # optional or default connection
    tc.connect()
    with open('rel_push_count_down.sql') as f:
        tc.execute(query=None, client='mclient', stdin=f)\
            .assertMatchStableOut(fout='rel_push_count_down.stable.out%s' % (suffix))\
            .assertMatchStableError(ferr='rel_push_count_down.stable.err')
