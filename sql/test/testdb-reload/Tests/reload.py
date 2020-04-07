'''\
Load the "dump" from the sql/test/testdb test and dump it.  Load that
dump into a fresh database and check the result of dumping that.
'''

import os, sys, socket, shutil
try:
    from MonetDBtesting import process
except ImportError:
    import process

dbfarm = os.getenv('GDK_DBFARM')
tstdb = os.getenv('TSTDB')
tstsrcdir = os.getenv('TSTSRCDIR')

if not tstdb or not dbfarm:
    print('No TSTDB or GDK_DBFARM in environment')
    sys.exit(1)

def freeport():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('', 0))
    port = sock.getsockname()[1]
    sock.close()
    return port

port2 = freeport()
tstdb2 = tstdb + '-clone'
if os.path.exists(os.path.join(dbfarm, tstdb2)):
    shutil.rmtree(os.path.join(dbfarm, tstdb2))

# start the first server
with process.server(stdin=process.PIPE,
                        stdout=process.PIPE,
                        stderr=process.PIPE) as s1:
    # load data into the first server's database
    with process.client(lang='sql',
                        server=s1,
                        args=[os.path.join(tstsrcdir, os.pardir, os.pardir, 'testdb', 'Tests', 'load.sql')],
                        stdin=process.PIPE,
                        stdout=process.DEVNULL,
                        stderr=process.DEVNULL) as c1:
        c1out, c1err = c1.communicate()
    # start the second server
    with process.server(dbname=tstdb2,
                        mapiport=port2,
                        stdin=process.PIPE,
                        stdout=process.PIPE,
                        stderr=process.PIPE) as s2:
        # dump the first server's database
        # and pipe it straight into the second server
        with process.client(lang='sqldump',
                            server=s1,
                            stdin=process.PIPE,
                            stdout='PIPE',
                            stderr=process.DEVNULL) as d1, \
             process.client(lang='sql',
                        server=s2,
                        stdin=d1.stdout,
                        stdout=process.DEVNULL,
                        stderr=process.DEVNULL) as c2:
            d1.stdout.close()
            d1.stdout = None
            c2out, c2err = c2.communicate()
            d1out, d1err = d1.communicate()
        s1out, s1err = s1.communicate()
        sys.stdout.write(s1out)
        sys.stderr.write(s1err)

        # dump the second server's database
        with process.client(lang='sqldump',
                            server=s2) as d2:
            d2out, d2err = d2.communicate()
        s2out, s2err = s2.communicate()
        sys.stdout.write(s2out)
        sys.stderr.write(s2err)
