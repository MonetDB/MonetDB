###
# Check that multiple processes can create and query a merge table with the
# same base table without crashing or hanging mserver5.
#
# If everything went well, this test produces no output
###

import pymonetdb
import os, sys
import multiprocessing as mp
import traceback
import random
import time

# The job for worker process
def exec_query(pid):
    dbh = None
    try:
        # sleep for some random time less than 1 sec. to avoid immediate
        # transaction conflicts on the `create merge table`
        time.sleep(random.random())

        dbh = pymonetdb.connect(database = os.environ['TSTDB'], port = int(os.environ['MAPIPORT']), hostname = os.environ['MAPIHOST'], autocommit=True)
        #dbh = pymonetdb.connect(database = 'demo', port=60000, autocommit=True)
        cur = dbh.cursor()
        cur.execute('create merge table concurrent_mt.cncrnt_mrg_tbl{} (like concurrent_mt.base_tbl);'.format(pid))
        cur.execute('alter table concurrent_mt.cncrnt_mrg_tbl{} add table base_tbl;'.format(pid))

        rowcnt = cur.execute('select * from concurrent_mt.cncrnt_mrg_tbl{};'.format(pid))
        if rowcnt != 2:
            raise ValueError('select * from concurrent_mt.cncrnt_mrg_tbl{}: 2 rows expected, got {}: {}'\
                    .format(pid, rowcnt, str(cur.fetchall())))

        cur.execute('drop table concurrent_mt.cncrnt_mrg_tbl{};'.format(pid))
    except pymonetdb.exceptions.Error as e:
        if "transaction is aborted because of concurrency conflicts" not in str(e):
            print(e)
    finally:
        if dbh is not None:
            dbh.close()

if __name__ == '__main__':
    mstdbh = None
    try:
        mstdbh = pymonetdb.connect(database = os.environ['TSTDB'], port = int(os.environ['MAPIPORT']), hostname = os.environ['MAPIHOST'], autocommit=True)
        #mstdbh = pymonetdb.connect(database = 'demo', port=60000, autocommit=True)
        mstcur = mstdbh.cursor()

        try:
            # This clean up is to make this test script usable standalone
            mstcur.execute('drop schema concurrent_mt cascade;')
            rowcnt = mstcur.execute('select name from sys.tables where name like \'%_tbl%\';')
            if rowcnt != 0:
                raise ValueError('Leftover tables before: {}'.format(str(mstcur.fetchall())))
        except Exception:
            pass
        mstcur.execute('create schema concurrent_mt;')
        # The base table to be shared by the merge tables.
        mstcur.execute('create table concurrent_mt.base_tbl (i int);')
        mstcur.execute('insert into concurrent_mt.base_tbl values (42), (24);')

        ### Setup a list of processes that we want to run
        # More processes can cause mserver5 to crash
        jobs = [mp.Process(target=exec_query, args=(x,)) for x in range(0, 50)]
        # Run processes
        [p.start() for p in jobs]
        # Wait for them to finish
        [p.join() for p in jobs]

        # Less processes can cause mserver5 to hang
        jobs = [mp.Process(target=exec_query, args=(x,)) for x in range(100, 105)]
        # Run processes
        [p.start() for p in jobs]
        # Wait for them to finish
        [p.join() for p in jobs]

        # Clean up
        mstcur.execute('drop schema concurrent_mt cascade;')
        rowcnt = mstcur.execute('select name from sys.tables where name like \'%_tbl%\';')
        if rowcnt != 0:
            raise ValueError('Leftover tables after: {}'.format(str(mstcur.fetchall())))
    except pymonetdb.exceptions.Error as e:
        print(e)
    finally:
        if mstdbh is not None:
            mstdbh.close()

