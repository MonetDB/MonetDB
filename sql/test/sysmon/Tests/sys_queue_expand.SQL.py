import pymonetdb
import os, sys
import multiprocessing as mp
import traceback
import time

# The job for worker process
def exec_query():
    dbh = None
    try:
        dbh = pymonetdb.connect(database = os.environ['TSTDB'], port = int(os.environ['MAPIPORT']), hostname = os.environ['MAPIHOST'], autocommit=True)
        #dbh = pymonetdb.connect(database = 'demo', autocommit=True)
        cur = dbh.cursor()
        cur.execute('call sys.sleep(3000)')
    except pymonetdb.exceptions.Error as e:
        print(e)
    finally:
        if dbh is not None:
            dbh.close()


mstdbh = None
try:
    mstdbh = pymonetdb.connect(database = os.environ['TSTDB'], port = int(os.environ['MAPIPORT']), hostname = os.environ['MAPIHOST'], autocommit=True)
    #mstdbh = pymonetdb.connect(database = 'demo', autocommit=True)
    mstcur = mstdbh.cursor()

    rowcnt = mstcur.execute('select \'before\', username,status,query from sys.queue() where status = \'running\' order by status, query')
    print("Before sleep: {no}".format(no=rowcnt))
    [print(row) for row in mstcur.fetchall()]

    # Setup a list of processes that we want to run
    jobs = [mp.Process(target=exec_query, args=()) for x in range(3)]
    # Run processes
    [p.start() for p in jobs]

    time.sleep(1)
    rowcnt = mstcur.execute('select \'during\', username,status,query from sys.queue() where status = \'running\' order by status, query')
    print("\nDuring sleep: {no}".format(no=rowcnt))
    [print(row) for row in mstcur.fetchall()]

    # Exit the completed processes
    [p.join() for p in jobs]

    rowcnt = mstcur.execute('select \'after\', username,status,query from sys.queue() where status = \'running\' order by status, query')
    print("\nAfter sleep: {no}".format(no=rowcnt))
    [print(row) for row in mstcur.fetchall()]

except pymonetdb.exceptions.Error as e:
    print(e)
finally:
    if mstdbh is not None:
        mstdbh.close()

