'''\
Load the "dump" from the sql/test/testdb test and dump it.  Load that
dump into a fresh database and check the result of dumping that.
'''

import os, sys, socket, shutil
try:
    from MonetDBtesting import process
except ImportError:
    import process
try:
    from MonetDBtesting import sqllogictest
except ImportError:
    import sqllogictest

dbfarm = os.getenv('GDK_DBFARM')
tstdb = os.getenv('TSTDB')
tstsrcdir = os.getenv('TSTSRCDIR')

if not tstdb or not dbfarm:
    print('No TSTDB or GDK_DBFARM in environment')
    sys.exit(1)

tstdb2 = tstdb + '-clone'
if os.path.exists(os.path.join(dbfarm, tstdb2)):
    shutil.rmtree(os.path.join(dbfarm, tstdb2))

# start the first server
with process.server(stdin=process.PIPE,
                    stdout=process.PIPE,
                    stderr=process.PIPE,
                    mapiport='0') as s1:
    # load data into the first server's database
    with sqllogictest.SQLLogic(out=open(os.devnull, 'w')) as sql:
        sql.connect(hostname='localhost',
                    port=s1.dbport,
                    database=s1.dbname)
        sql.parse(os.path.join(tstsrcdir, os.pardir, os.pardir,
                               'testdb', 'Tests', 'load.test'))
    # start the second server
    with process.server(dbname=tstdb2,
                        mapiport='0',
                        stdin=process.PIPE,
                        stdout=process.PIPE,
                        stderr=process.PIPE) as s2:
        # dump the first server's database
        # and pipe it straight into the second server
        with process.client(lang='sqldump',
                            server=s1,
                            stdin=process.PIPE,
                            stdout='PIPE',
                            stderr=process.PIPE) as d1, \
             process.client(lang='sql',
                            format='trash',
                            echo=False,
                            server=s2,
                            stdin=d1.stdout,
                            stdout=process.PIPE,
                            stderr=process.PIPE) as c2:
            d1.stdout.close()
            d1.stdout = None
            c2out, c2err = c2.communicate()
            d1out, d1err = d1.communicate()
            sys.stderr.write(c2err)
            sys.stderr.write(d1err)
        s1out, s1err = s1.communicate()
        sys.stdout.writelines([line for line in s1out.splitlines(keepends=True) if not line.startswith('#')])
        sys.stderr.writelines([line for line in s1err.splitlines(keepends=True) if not line.startswith('#')])

        # dump the second server's database
        with process.client(lang='sqldump',
                            server=s2,
                            stdin=process.PIPE,
                            stdout=process.PIPE,
                            stderr=process.PIPE) as d2:
            d2out, d2err = d2.communicate()
            sys.stderr.write(d2err)
        s2out, s2err = s2.communicate()
        sys.stdout.writelines([line for line in s2out.splitlines(keepends=True) if not line.startswith('#')])
        sys.stderr.writelines([line for line in s2err.splitlines(keepends=True) if not line.startswith('#')])

if len(sys.argv) == 2 and sys.argv[1] == 'reload':
    output = ''.join(d2out).splitlines(keepends=True)
    while len(output) > 0 and output[0].startswith('--'):
        del output[0]
    stableout = os.path.join(tstsrcdir, '../../testdb/Tests/dump-nogeom.stable.out')
    stable = open(stableout, encoding='utf-8').readlines()
    import difflib
    for line in difflib.unified_diff(stable, output, fromfile='test', tofile=stableout):
        sys.stderr.write(line)
else:
    sys.stdout.writelines(d2out)
