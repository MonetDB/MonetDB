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

dbname = tstdb

s = process.server(dbname = dbname, stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)

c = process.client('sql', server = s, stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)

cout, cerr = c.communicate('''\
select * from tmp;
delete from tmp where i < 4;
select * from tmp;
''')

sout, serr = s.communicate()

sys.stdout.write(sout)
sys.stdout.write(cout)
sys.stderr.write(serr)
sys.stderr.write(cerr)

def listfiles(path):
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

listfiles(dbfarm + os.path.sep + tstdb)
listfiles(dbfarm + os.path.sep + tstdb + os.path.sep + 'wlc_logs')
