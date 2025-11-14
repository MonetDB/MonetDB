import tarfile
from MonetDBtesting import process
import pymonetdb
import sys
import shutil
import os
import contextlib
import copy


dbfarm = os.getenv('GDK_DBFARM')
tstdb = os.getenv('TSTDB')
mydb = tstdb + '_snapx'


def log(message):
    assert '\n' not in message
    print(f'##      {message}', flush=True)


def query_one(conn, sql, args=None):
    with conn.cursor() as c:
        c.execute(sql, args)
        return c.fetchone()[0]


@contextlib.contextmanager
def runserver():
    """Run an mserver, optionally clean its database directory afterward

    Returns a connection to the database
    """

    log('starting server')
    # why the stdin=PIPE?
    with process.server(dbname=mydb, mapiport=0, stdin=process.PIPE) as server:
        port = server.dbport
        log(f'port={port}')

        def do_connect(): return pymonetdb.connect(
            database=server.usock or mydb, port=port, username='monetdb', password='monetdb')

        with do_connect() as conn:
            dbpath = query_one(
                conn, "select value from sys.environment where name = 'gdk_dbpath'")
            conn.dbpath = dbpath   # add additional attributes
            conn.another_connection = do_connect
            yield conn
    log(f'cleaning {dbpath}')
    shutil.rmtree(dbpath)


foo_cols = "i INT PRIMARY KEY, t TEXT"
foo_data = [(1, 'one'), (2, 'two'), (3, 'three')]
foo_update = "UPDATE foo SET t = t || '!'"
foo_data2 = [(i, t + '!') for i, t in foo_data]

bar_cols = "j INT, i INT REFERENCES foo(i)"
bar_data = [(11, 1), (21, 1), (32, 2)]
bar_update = "UPDATE bar SET j = j * 10"
bar_data2 = [(j * 10, i) for j, i in bar_data]

baz_cols = "x TEXT"
baz_data = [('hello, world',)]


def initialize_table(conn, name, cols, data, *, unlogged=False):
    table = "UNLOGGED TABLE" if unlogged else "TABLE"
    with conn.cursor() as c:
        log(f'CREATE {table} {name}')
        c.execute(f"CREATE {table} {name} ({cols})")
        placeholders = ','.join(['%s'] * len(data[0]))
        ins = f"INSERT INTO {name} VALUES ({placeholders})"
        for row in data:
            c.execute(ins, row)


def verify_table(conn, name, data):
    log(f'verifying {name} contains {data!r}')
    with conn.cursor() as c:
        expected = sorted(copy.deepcopy(data))
        c.execute(f"SELECT * FROM {name}")
        actual = sorted(c.fetchall())
        if actual != expected:
            raise Exception(
                f'table {name}: expected {expected!r}, got {actual!r}')


def make_snapshot(conn, filename, omit_unlogged, omit_tables):
    log(f'snapshot {filename!r}: omit_unlogged={omit_unlogged} omit_tables={omit_tables!r}')
    try:
        os.remove(filename)
    except FileNotFoundError:
        pass
    schema_id = query_one(
        conn, "SELECT id FROM sys.schemas WHERE name = CURRENT_SCHEMA")
    omit_ids = []
    for name in omit_tables:
        id = query_one(conn, "SELECT id FROM sys.tables WHERE name = %s AND schema_id = %s", [
                       name, schema_id])
        omit_ids.append(id)
    with conn.cursor() as c:
        joined_omit_ids = ','.join(str(id) for id in omit_ids)
        c.execute("CALL sys.hot_snapshot(%s, true, %s, %s)",
                  (filename, omit_unlogged, joined_omit_ids))


def unpack_snapshot(filename):
    log(f'unpacking snapshot {filename}')
    with tarfile.open(filename) as tar:
        try:
            tar.extraction_filter
        except AttributeError:
            # pre 3.12 Python
            tar.extractall(dbfarm)
        else:
            tar.extractall(dbfarm, filter='data')


shutil.rmtree(os.path.join(dbfarm, mydb), ignore_errors=True)
# Create the database and set up some test data.
# Instantly create a full snapshot.
# Verify that foreign key constraints are checked.
with runserver() as conn:
    initialize_table(conn, 'foo', foo_cols, foo_data)
    initialize_table(conn, 'bar', bar_cols, bar_data)
    initialize_table(conn, 'baz', baz_cols, baz_data, unlogged=True)
    conn.commit()
    snapshot_file = conn.dbpath + '.tar'
    make_snapshot(conn, snapshot_file, False, [])
    assert os.path.exists(snapshot_file)
    #
    try:
        # because bar has a foreign key constraint on foo, foo cannot be omitted
        make_snapshot(conn, conn.dbpath + '.willfail.tar', False, ['foo'])
        assert False and "should have failed because bar depends on foo"
    except pymonetdb.OperationalError:
        log('snapshot failed as expected')


# Unpack tar file.
# Start, which flushes the WAL.
# Check all data is available.
# Create some new snapshots.
# Commit updates while another transaction is still observing the old state.
# Make snapshots with that data in the WAL.
log('------------------------------------------------------')
unpack_snapshot(snapshot_file)
with runserver() as conn1:
    verify_table(conn1, 'foo', foo_data)
    verify_table(conn1, 'bar', bar_data)
    verify_table(conn1, 'baz', baz_data)
    #
    snapshot_file_omit_foo_bar = conn1.dbpath + '.omit_foo_bar.tar'
    make_snapshot(conn1, snapshot_file_omit_foo_bar, False, ['foo', 'bar'])
    snapshot_file_omit_bar = conn1.dbpath + '.omit_bar.tar'
    make_snapshot(conn1, snapshot_file_omit_bar, False, ['bar'])
    snapshot_file_omit_unlogged = conn1.dbpath + '.omit_unlogged.tar'
    make_snapshot(conn1, snapshot_file_omit_unlogged, True, [])
    #
    with conn1.another_connection() as conn2, conn2.cursor() as c2:
        # observe the old state
        log('observing current state from another transaction')
        c2.execute(
            "SELECT COUNT(*) FROM foo; SELECT COUNT(*) FROM bar; SELECT COUNT(*) FROM baz")
        #
        # commit new state on conn1
        with conn1.cursor() as c1:
            log('updating state on main transaction')
            c1.execute(foo_update)
            c1.execute(bar_update)
            conn1.commit()
        verify_table(conn1, 'foo', foo_data2)
        verify_table(conn1, 'bar', bar_data2)
        verify_table(conn2, 'foo', foo_data)
        verify_table(conn2, 'bar', bar_data)
        #
        snapshot_file_wal = conn1.dbpath + '.wal.tar'
        make_snapshot(conn1, snapshot_file_wal, False, [])
        snapshot_file_omit_bar_wal = conn1.dbpath + '.omit_bar_wal.tar'
        make_snapshot(conn1, snapshot_file_omit_bar_wal,
                      False, ['bar'])


# Unpack _omit_foo_bar and verify the contents
log('------------------------------------------------------')
unpack_snapshot(snapshot_file_omit_foo_bar)
with runserver() as conn:
    # foo and bar have been omitted so they are empty
    verify_table(conn, 'foo', [])
    verify_table(conn, 'bar', [])
    verify_table(conn, 'baz', baz_data)


# Unpack _omit_bar and verify the contents
log('------------------------------------------------------')
unpack_snapshot(snapshot_file_omit_bar)
with runserver() as conn:
    # bar has been omitted, foo is still present
    verify_table(conn, 'foo', foo_data)
    verify_table(conn, 'bar', [])
    verify_table(conn, 'baz', baz_data)


# Unpack _omit_unlogged and verify the contents
log('------------------------------------------------------')
unpack_snapshot(snapshot_file_omit_unlogged)
with runserver() as conn:
    # baz is unlogged so it has been omitted
    verify_table(conn, 'foo', foo_data)
    verify_table(conn, 'bar', bar_data)
    verify_table(conn, 'baz', [])


# Unpack _wal and verify the contents
log('------------------------------------------------------')
unpack_snapshot(snapshot_file_wal)
with runserver() as conn:
    # observe updated tables
    verify_table(conn, 'foo', foo_data2)
    verify_table(conn, 'bar', bar_data2)


# Unpack _omit_foo_bar_wal and verify the contents
log('------------------------------------------------------')
unpack_snapshot(snapshot_file_omit_bar_wal)
with runserver() as conn:
    # observe updated tables
    verify_table(conn, 'foo', foo_data2)
    verify_table(conn, 'bar', [])
