from os import environ
from MonetDBtesting import tpymonetdb as pymonetdb

conn = pymonetdb.connect(database=environ['TSTDB'], port=environ['MAPIPORT'], autocommit=True)

def count_dirty_voids(conn):
    sql = "SELECT COUNT(*) FROM sys.bbp() WHERE dirty = 'dirty' AND ttype = 'void'"
    with conn.cursor() as c:
        c.execute(sql)
        return c.fetchone()[0]

# prepare
nrows = 500_000   # problem used to occur starting at 100_001
with conn.cursor() as c:
    c.execute("DROP TABLE IF EXISTS foo")
    c.execute("CREATE TABLE foo(i INT, j INT, k INT)")
    c.execute("INSERT INTO foo SELECT value AS i, -value AS j, 2 * value AS k FROM sys.generate_series(0, %s)", [nrows])


def do_it(conn):
    with conn.cursor() as c:
        c.execute("PREPARE SELECT i AS col1, j as COL2 FROM foo LIMIT 1")
        prep_id = c.lastrowid
        c.execute("EXECUTE %s()", [prep_id])
        c.execute("DEALLOCATE %s", [prep_id])


# run it a few times
dirty_void_counts = []
dirty_void_counts.append(count_dirty_voids(conn))
for i in range(4):
    do_it(conn)
    dirty_void_counts.append(count_dirty_voids(conn))

# In principle, there should not be any new dirty void bats.
# But maybe one or to appear because of other things that are
# happening in the server.  Therefore we accept this test as
# succesful if at least one pair of measurements does not show
# an increase

print('# These are the dirty void bat counts we measured: ', dirty_void_counts)
for i in range(len(dirty_void_counts) - 1):
    if dirty_void_counts[i] >= dirty_void_counts[i + 1]:
        print("# It doesn't always increase so it's probably fine")
        exit(0)
# if we get here, they were all increasing
print('# They keep increasing so there is a leak')
exit(1)
