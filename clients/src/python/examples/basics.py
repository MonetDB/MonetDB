
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
c.arraysize=1
c.execute('select * from tables')
results = c.fetchmany()
print len(results)
c.arraysize=3
results = c.fetchmany()
print len(results)


for arraysize in (1,10,100,1000):
    t = time.time()
    c.arraysize = arraysize
    c.execute('select * from tables, tables, tables')
    results = c.fetchall()
    print arraysize, time.time() -t

