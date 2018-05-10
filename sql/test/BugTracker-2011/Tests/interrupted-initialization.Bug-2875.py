from __future__ import print_function

import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

dbfarm = os.getenv('GDK_DBFARM')
tstdb = os.getenv('TSTDB')

if not tstdb or not dbfarm:
    print('No TSTDB or GDK_DBFARM in environment')
    sys.exit(1)

dbname = tstdb + '-bug2875'

# clean up before we start
if os.path.exists(os.path.join(dbfarm, dbname)):
    import shutil
    shutil.rmtree(os.path.join(dbfarm, dbname))

s = process.server(stdin = process.PIPE,
                   stdout = process.PIPE,
                   stderr = process.PIPE,
                   dbname = dbname)
out, err = s.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
s = process.server(stdin = process.PIPE,
                   stdout = process.PIPE,
                   stderr = process.PIPE,
                   dbname = dbname)
c = process.client(lang = 'sqldump',
                   stdin = process.PIPE,
                   stdout = process.PIPE,
                   stderr = process.PIPE,
                   dbname = dbname)
out, err = c.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
out, err = s.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
