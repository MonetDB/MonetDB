
import logging
import time

#configure the logger, so we can see what is happening
#logging.getLogger().setLevel(logging.DEBUG)

try:
    import monetdb.sql
except ImportError:
    # running examples from development tree
    import sys, os
    parent = os.path.join(sys.path[0], '..')
    sys.path.append(parent)
    import monetdb.sql


x = monetdb.sql.connect()
c = x.cursor()

# some basic query
#c.arraysize=1
#c.execute('select * from tables')
#results = c.fetchmany()
#c.arraysize=3
#results = c.fetchmany()


for arraysize in (100,1000,10000, 100000):
    t = time.time()
    c.arraysize = arraysize
    c.execute('select * from tables, tables, tables, tables')
    results = c.fetchall()
    print len(results)
    print arraysize, time.time() -t

