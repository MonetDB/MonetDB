try:
    from MonetDBtesting import process
except ImportError:
    import process

import sys, time, pymonetdb, os

def connect(autocommit):
    return pymonetdb.connect(database = os.getenv('TSTDB'),
                             hostname = '127.0.0.1',
                             port = int(os.getenv('MAPIPORT')),
                             username = 'monetdb',
                             password = 'monetdb',
                             autocommit = autocommit)

def query(conn, sql):
    cur = conn.cursor()
    cur.execute(sql)
    r = cur.fetchall()
    cur.close()
    return r

# no timeout since we need to kill mserver5, not the inbetween Mtimeout
with process.server(stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE) as s:
    # boring setup and schema creation stuff:
    c1 = connect(True)
    c1.execute('create table foo (a int)')
    c1.execute('create table bar (a int)')
    c1.execute('insert into foo values (1),(2),(3)')
    if query(c1, 'select * from foo') != [(1,), (2,), (3,)]:
        sys.stderr.write('Expected [(1,), (2,), (3,)]')

    # Run 'delete from foo' with store_nr_active > 1
    # This causes MonetDB to allocate a new file for foo rather than
    # wiping the existing one
    c2 = connect(True)
    c2.execute('start transaction')
    c1.execute('delete from foo')
    c2.execute('rollback')

    # Populate some new data into foo, and demonstrate that a new file has
    # been allocated
    c1.execute('insert into foo values (4),(5),(6)')

    # Generate at least 1000 changes, as required by store_manager() in
    # order to cause a logger restart
    c1.execute('insert into bar select * from generate_series(cast(0 as int),1500)')

    # Wait at least 30 seconds in order to ensure store_manager() has done
    # the logger restart
    # An alternative would have been to generate at least SNAPSHOT_MINSIZE
    # rows in one statement, but this way is simpler
    time.sleep(1)
    if query(c1, 'select * from foo') != [(4,), (5,), (6,)]:
        sys.stderr.write('Expected [(4,), (5,), (6,)]')

    s.communicate()
    c2.close()

with process.server(stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE) as t:
    c3 = connect(True)
    # This prints the wrong data. It should print exactly the same as the
    # previous line: "[(4,), (5,), (6,)]" , but actually prints "[(1,),
    # (2,), (3,)]"
    if query(c1, 'select * from foo') != [(4,), (5,), (6,)]:
        sys.stderr.write('Expected [(4,), (5,), (6,)]')

    # cleanup
    c3.execute('drop table foo')
    c3.execute('drop table bar')

    t.communicate()
    c1.close()
    c3.close()
