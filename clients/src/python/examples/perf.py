
import logging
import time

#configure the logger, so we can see what is happening
#logging.getLogger().setLevel(logging.DEBUG)

try:
    import monetdb.sql
except ImportError:
    # running examples from development tree
    import sys
    import os
    parent = os.path.join(sys.path[0], '..')
    sys.path.append(parent)
    import monetdb.sql

for i in (10, 100, 1000, 10000):
    t = time.time()
    x = monetdb.sql.connect()
    c = x.cursor()
    c.arraysize=i
    c.execute('select * from tables, tables, tables, tables')
    results = c.fetchall()
    print i, time.time() - t

#import cProfile
#cProfile.run('query()')
