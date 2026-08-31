'''
Load the "dump" from the sql/test/testdb test and snapshot it.  Load that
snapshot into a fresh database and check the result of dumping that.
'''

import os, sys, shutil, tempfile, tarfile
try:
    from MonetDBtesting import process
except ImportError:
    import process
try:
    from MonetDBtesting import sqllogictest
except ImportError:
    import sqllogictest
try:
    from MonetDBtesting import tpymonetdb
except ImportError:
    import tpymonetdb

dbfarm = os.getenv('GDK_DBFARM')
tstdb = os.getenv('TSTDB')
tstsrcdir = os.getenv('TSTSRCDIR')
SCRATCH_PREFIX = os.getenv('TSTTRGDIR', None)

if not tstdb or not dbfarm:
    print('No TSTDB or GDK_DBFARM in environment')
    sys.exit(1)

tstdb2 = tstdb + '-clone'
if os.path.exists(os.path.join(dbfarm, tstdb2)):
    shutil.rmtree(os.path.join(dbfarm, tstdb2))


def tar_filter(member, path):
    # remove database name from file name
    member.name = member.name.partition('/')[2]
    return member


with tempfile.TemporaryDirectory(dir=SCRATCH_PREFIX) as tmpdir:
    tar_file = os.path.join(tmpdir, 'dump.tar.xz')
    # start the first server
    with process.server(dbfarm=dbfarm,
                        dbname=tstdb,
                        mapiport='0',
                        stdin=process.PIPE,
                        stdout=process.PIPE,
                        stderr=process.PIPE) as s1:
        # load data into the first server's database
        with sqllogictest.SQLLogic(out=None) as sql:
            sql.connect(server=s1)
            sql.parse(os.path.join(tstsrcdir, os.pardir, os.pardir,
                                   'testdb', 'Tests', 'load.test'))
        # dump the first server's database into tar_file
        with tpymonetdb.connect(database=s1.urls[0]) as con:
            with con.cursor() as c:
                c.execute('call sys.hot_snapshot(%s, 1)', [tar_file])
    os.mkdir(tstdb2)
    with tarfile.open(name=tar_file) as tar:
        tar.extractall(os.path.join(dbfarm, tstdb2), filter=tar_filter)
    # start the second server
    with process.server(dbfarm=dbfarm,
                        dbname=tstdb2,
                        mapiport='0',
                        stdin=process.PIPE,
                        stdout=process.PIPE,
                        stderr=process.PIPE) as s2:
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
    stableout = os.path.join(tstsrcdir, os.pardir, os.pardir,
                             'testdb', 'Tests', 'dump-nogeom.stable.out')
    with open(stableout, encoding='utf-8') as fil:
        stable = fil.readlines()
    import difflib
    for line in difflib.unified_diff(stable, output,
                                     fromfile='expected', tofile='received'):
        sys.stderr.write(line)
else:
    sys.stdout.writelines(d2out)
