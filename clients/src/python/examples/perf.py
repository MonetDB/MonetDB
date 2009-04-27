
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

def query():
    x = monetdb.sql.connect()
    c = x.cursor()
    c.arraysize=1000
    c.execute('select * from tables, tables, tables')
    results = c.fetchall()

import cProfile
cProfile.run('query()')
