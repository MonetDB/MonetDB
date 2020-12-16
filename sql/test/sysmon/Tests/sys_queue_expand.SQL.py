###
# Check that when a new query is executed while sys.queue() is still filled
#   with unfinished queries (e.g. RUNNING, PAUZED and PREPARED(?)), sys.queue()
#   will automatically expand the queue size
# Need multiprocessing for the long-running queries
###

import pymonetdb
import os
import multiprocessing as mp

db = os.environ['TSTDB']
pt = int(os.environ['MAPIPORT'])
SLEEP_TIME='5000'

def worker_task():
    dbh = None
    try:
        dbh = pymonetdb.connect(database=db, port=pt, autocommit=True)
        cur = dbh.cursor()
        cur.execute('call sys.sleep('+SLEEP_TIME+')')
    except pymonetdb.exceptions.Error as e:
        print(e)
    finally:
        if dbh is not None:
            dbh.close()

def do_error(query, res, expected_res):
    print("query text:\n" + query)
    print("query results:\n" + str(res))
    print("expected:\n" + str(expected_res))

def main():
    mstdbh = None
    mstcur = None
    try:
        mstdbh = pymonetdb.connect(database=db, port=pt, autocommit=True)
        mstcur = mstdbh.cursor()

        query = 'create procedure sleep(i int) external name alarm.sleep;'
        mstcur.execute(query)

        query = 'select username, status, query from sys.queue() where status = \'running\' order by query'
        expected_res = [('monetdb', 'running', 'select username, status, query from sys.queue() where status = \\\'running\\\' order by query\n;')]
        rowcnt = mstcur.execute(query)
        res = mstcur.fetchall()
        if rowcnt != len(expected_res) or res != expected_res:
            do_error(query, res, expected_res)

        # Setup a list of processes that we want to run
        jobs = [mp.Process(target=worker_task, args=()) for x in range(3)]
        # Run processes
        [p.start() for p in jobs]

        # Check the long running query, but lets first wait for a moment for the
        #   workers to start with their queries
        mstcur.execute('call sys.sleep(500)')
        query = 'select username, status, query from sys.queue() where query like \'call sys.sleep(5000)%\' order by query'
        expected_res = [
        ('monetdb', 'running', 'call sys.sleep('+SLEEP_TIME+')\n;'),
        ('monetdb', 'running', 'call sys.sleep('+SLEEP_TIME+')\n;'),
        ('monetdb', 'running', 'call sys.sleep('+SLEEP_TIME+')\n;')]
        rowcnt = mstcur.execute(query)
        res = mstcur.fetchall()
        if rowcnt != len(expected_res) or res != expected_res:
            do_error(query, res, expected_res)

        # Exit the completed processes
        [p.join() for p in jobs]

        # sys.queue() should have been expanded from 4 to 8, so we should be able
        #   to have 7 queries in the queue
        mstcur.execute('select 6')
        mstcur.execute('select 7')
        query = 'select count(*) from sys.queue()'
        expected_res = 7
        rowcnt = mstcur.execute(query)
        res = mstcur.fetchall()
        if rowcnt != 1 or res[0][0] != expected_res:
            do_error(query, res, expected_res)
    except pymonetdb.exceptions.Error as e:
        print(e)
    finally:
        if mstdbh is not None:
            if mstcur is not None:
                mstcur.execute('drop procedure sleep;')
            mstdbh.close()

if __name__ == "__main__":
    main()
