from __future__ import print_function

try:
    from MonetDBtesting import process
except ImportError:
    import process
import os, sys

dbfarm = os.getenv('GDK_DBFARM')
tstdb = os.getenv('TSTDB')

if not tstdb or not dbfarm:
    print('No TSTDB or GDK_DBFARM in environment')
    sys.exit(1)

#clean up first
dbname = tstdb
dbnameclone = tstdb + '-clone'

s = process.server(dbname = dbname, stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)

c = process.client('sql', dbname = dbname, stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)

#continue logging
cout, cerr = c.communicate('''\
create table tmp70(i int, s string);
insert into tmp70 values(1,'hello'), (2,'world');
select * from tmp70;
''')

sout, serr = s.communicate()

sys.stdout.write(sout)
sys.stdout.write(cout)
sys.stderr.write(serr)
sys.stderr.write(cerr)
