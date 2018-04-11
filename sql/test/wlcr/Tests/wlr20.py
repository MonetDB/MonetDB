from __future__ import print_function

try:
    from MonetDBtesting import process
except ImportError:
    import process
import os, sys, socket, time

dbfarm = os.getenv('GDK_DBFARM')
tstdb = os.getenv('TSTDB')

if not tstdb or not dbfarm:
    print('No TSTDB or GDK_DBFARM in environment')
    sys.exit(1)

def freeport():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('', 0))
    port = sock.getsockname()[1]
    sock.close()
    return port

cloneport = freeport()

dbname = tstdb
dbnameclone = tstdb + '-clone'

# master = process.server(dbname = dbname, stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
slave = process.server(dbname = dbnameclone, mapiport = cloneport, stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)

c = process.client('sql', dbname = dbnameclone, port = cloneport, stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)

#two step roll forward, where first step shouldn't do anything because already in previous test
cout, cerr = c.communicate('''\
call replicate('%s',1);
select * from tmp;
call replicate('%s',2);
select * from tmp;
call replicate('%s',4);
select * from tmp;
''' % (dbname,dbname,dbname))

sout, serr = slave.communicate()
#mout, merr = master.communicate()

#sys.stdout.write(mout)
sys.stdout.write(sout)
sys.stdout.write(cout)
#sys.stderr.write(merr)
sys.stderr.write(serr)
sys.stderr.write(cerr)
