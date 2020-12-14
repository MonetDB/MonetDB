from MonetDBtesting.sqltest import SQLTestCase
try:
    from MonetDBtesting import process
except ImportError:
    import process
import os, sys

DBFARM = os.getenv('GDK_DBFARM')
TSTDB = os.getenv('TSTDB')
MAPIPORT = os.getenv('MAPIPORT')

if not TSTDB or not DBFARM:
    print('No TSTDB or GDK_DBFARM in environment')
    sys.exit(1)

with process.server(mapiport=MAPIPORT, dbname=TSTDB, stdout=process.PIPE, stderr=process.PIPE) as s:
    with SQLTestCase() as tc:
            tc.connect(database=TSTDB, port=MAPIPORT)
            tc.execute("""
                call wlc.beat(0);
                call wlc.master();""").assertSucceeded()
            tc.execute("create table tmp0(i int, s string);""").assertSucceeded()
            tc.execute("insert into tmp0 values(1,'gaap'), (2,'sleep');")\
                    .assertSucceeded()\
                    .assertRowCount(2)
            tc.execute("drop table tmp0;").assertSucceeded()
            tc.execute("create table tmp(i int, s string);").assertSucceeded()
            tc.execute("insert into tmp values(1,'hello'), (2,'world');")\
                    .assertSucceeded()\
                    .assertRowCount(2)
            tc.execute("select * from tmp;")\
                    .assertSucceeded()\
                    .assertRowCount(2)\
                    .assertDataResultMatch(data=[(1, 'hello'), (2, 'world')])

    sout, serr = s.communicate()

#with process.server(dbname=dbname, stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as s, \
#     process.client('sql', server=s, stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as c:
#
#    cout, cerr = c.communicate('''\
#call wlc.beat(0);
#call wlc.master();
#
#create table tmp0(i int, s string);
#insert into tmp0 values(1,'gaap'), (2,'sleep');
#drop table tmp0;
#create table tmp(i int, s string);
#insert into tmp values(1,'hello'), (2,'world');
#select * from tmp;
#''')
#
#    sout, serr = s.communicate()
#
#    sys.stdout.write(sout)
#    sys.stdout.write(cout)
#    sys.stderr.write(serr)
#    sys.stderr.write(cerr)

def listfiles(path):
    sys.stdout.write("#LISTING OF THE LOG FILES\n")
    for f in sorted(os.listdir(path)):
        if (f.find('wlc') >= 0 or f.find('wlr') >=0 ) and f != 'wlc_logs':
            file = path + os.path.sep + f
            sys.stdout.write('#' + file + "\n")
            try:
                x = open(file)
                s = x.read()
                lines = s.split('\n')
                for l in lines:
                    sys.stdout.write('#' + l + '\n')
                x.close()
            except IOError:
                sys.stderr.write('Failure to read file ' + file + '\n')

listfiles(os.path.join(DBFARM, TSTDB))
listfiles(os.path.join(DBFARM, TSTDB, 'wlc_logs'))

