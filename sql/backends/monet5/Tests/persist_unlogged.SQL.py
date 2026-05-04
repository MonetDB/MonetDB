import os, tempfile, time
from MonetDBtesting import tpymonetdb as pymonetdb
from MonetDBtesting.sqltest import SQLTestCase

try:
    from MonetDBtesting import process
except ImportError:
    import process

COUNT_FOO = """
SELECT COUNT(*)
FROM foo
"""

PERSIST_FOO = """
SELECT tname, persisted_row_count
FROM persist_unlogged('sys', 'foo') r(tname, tid, persisted_row_count)
"""

# This test disables TESTINGMASK, keeping all other enabled flags.
# TESTINGMASK affects the number of minimum changes in a transaction that are
# needed to activate _flushnow_ flag.
# The purpose of activating _flushnow_ in a transaction is to avoid writing to
# WAL all changes made by that transaction AND sync those changes to persistent
# storage immediately.
# To achieve this, the WAL must be flushed to sync past transaction changes.
# Flushing WAL triggers a WAL file rotation.

# The requirement for a successful persistence of unlogged table data is that
# the table needs to be known as a persistent table by GDK.
# This is only guaranteed after a WAL flush! (See BBP_status)

# The test triggers the WAL flush by:
#       TEST A) double call to persist_unlogged function,
#               waiting 1 sec to give time for WAL flush
#       TEST B) triggered by another transaction that activated _flushnow_
#               and consequentially WAL flush

        # with pymonetdb.connect(port=s.dbport, database='db1', autocommit=True) as client:
            # with client.cursor() as cur:

def disable_testing_debug_flag(db):
    with pymonetdb.connect(port=s.dbport, database=db, autocommit=True) as client:
        with client.cursor() as cur:
            cur.execute("SELECT debug(0)")
            current_debub_flag = cur.fetchone()[0]
            rm_tsting_flag = current_debub_flag & ~256
            cur.execute(f"SELECT debug({rm_tsting_flag})")
            cur.execute(f"SELECT debug({rm_tsting_flag})")

# TEST A) double call to persist_unlogged function,
#         waiting 1 sec to give time for WAL flush
with tempfile.TemporaryDirectory() as tmp_dir:
    os.mkdir(os.path.join(tmp_dir, 'farm'))

    with process.server(dbfarm=os.path.join(tmp_dir, 'farm'), dbname='db1', mapiport='0',
                        stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as s:

        disable_testing_debug_flag('db1')

        with SQLTestCase(server=s) as tc:
            tc.execute("CREATE UNLOGGED TABLE foo (x INT)").assertSucceeded()
            tc.execute("ALTER TABLE foo SET INSERT ONLY").assertSucceeded()
            tc.execute("INSERT INTO foo SELECT * FROM generate_series(0, 500)")
            tc.execute(COUNT_FOO).assertSucceeded().assertDataResultMatch([(500,)])
            tc.execute(PERSIST_FOO).assertSucceeded().assertDataResultMatch([('foo', 0)])
            time.sleep(1)
            tc.execute(PERSIST_FOO).assertSucceeded().assertDataResultMatch([('foo', 500)])

        s.communicate()

    # checking if data is there after server restart
    with process.server(dbfarm=os.path.join(tmp_dir, 'farm'), dbname='db1', mapiport='0',
                        stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as s:

        with SQLTestCase(server=s) as tc:
            tc.execute("SELECT COUNT(*) FROM foo").assertSucceeded().assertDataResultMatch([(500,)])

        s.communicate()

# TEST B) triggered by another transaction that activated _flushnow_
#         and consequentially WAL flush
with tempfile.TemporaryDirectory() as tmp_dir:
    os.mkdir(os.path.join(tmp_dir, 'farm'))

    with process.server(dbfarm=os.path.join(tmp_dir, 'farm'), dbname='db2', mapiport='0',
                        stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as s:

        disable_testing_debug_flag('db2')

        with SQLTestCase(server=s) as tc:
            tc.execute("CREATE UNLOGGED TABLE foo (x INT)").assertSucceeded()
            tc.execute("ALTER TABLE foo SET INSERT ONLY").assertSucceeded()
            tc.execute("INSERT INTO foo SELECT * FROM generate_series(0, 500)")
            tc.execute(COUNT_FOO).assertSucceeded().assertDataResultMatch([(500,)])
            tc.execute("CREATE TABLE bar (x INT)").assertSucceeded()
            tc.execute("INSERT INTO bar SELECT * FROM generate_series(0, 1_000_000)").assertSucceeded()
            tc.execute(PERSIST_FOO).assertSucceeded().assertDataResultMatch([('foo', 500)])

        s.communicate()

    # checking if data is there after server restart
    with process.server(dbfarm=os.path.join(tmp_dir, 'farm'), dbname='db2', mapiport='0',
                        stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as s:

        with SQLTestCase(server=s) as tc:
            tc.execute("SELECT COUNT(*) FROM foo").assertSucceeded().assertDataResultMatch([(500,)])

        s.communicate()
