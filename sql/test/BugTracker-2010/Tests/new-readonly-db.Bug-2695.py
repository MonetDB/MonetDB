from __future__ import print_function

try:
    from MonetDBtesting import process
except ImportError:
    import process
import os

dbfarm = os.getenv('GDK_DBFARM')
tstdb = os.getenv('TSTDB')

if not tstdb or not dbfarm:
    print('No TSTDB or GDK_DBFARM in environment')
    sys.exit(1)

dbname = tstdb + '-2695'

# clean up before we start
if os.path.exists(os.path.join(dbfarm, dbname)):
    import shutil
    shutil.rmtree(os.path.join(dbfarm, dbname))

s = process.server(args = ['--readonly'], dbname = dbname)
s.wait()
