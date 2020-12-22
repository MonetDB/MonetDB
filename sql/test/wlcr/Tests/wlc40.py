try:
    from MonetDBtesting import process
except ImportError:
    import process
import os, sys
from MonetDBtesting.sqltest import SQLTestCase

dbfarm = os.getenv('GDK_DBFARM')
tstdb = os.getenv('TSTDB')

if not tstdb or not dbfarm:
    print('No TSTDB or GDK_DBFARM in environment')
    sys.exit(1)

dbname = tstdb

with process.server(dbname=dbname, stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as s, \
        SQLTestCase() as tc:
    tc.connect(database=dbname)
    tc.execute("select * from tmp;")\
        .assertSucceeded()\
        .assertDataResultMatch([(3, 'blah'), (2, 'blah'), (3, 'blah'), (4, 'blah'), (5, 'blah'), (6, 'blah')])
    tc.execute("delete from tmp where i < 4;")\
        .assertSucceeded()\
        .assertRowCount(3)
    tc.execute("select * from tmp;")\
        .assertSucceeded()\
        .assertDataResultMatch([(4, 'blah'), (5, 'blah'), (6, 'blah')])

    sout, serr = s.communicate()

#with process.server(dbname=dbname, stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as s, \
#     process.client('sql', server = s, stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE) as c:
#
#    cout, cerr = c.communicate('''\
#select * from tmp;
#delete from tmp where i < 4;
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

listfiles(os.path.join(dbfarm, tstdb))
listfiles(os.path.join(dbfarm, tstdb, 'wlc_logs'))
