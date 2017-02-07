try:
    from MonetDBtesting import process
except ImportError:
    import process
import os, sys

dbfarm = os.getenv('GDK_DBFARM')
tstdb = os.getenv('TSTDB')

if not tstdb or not dbfarm:
    print 'No TSTDB or GDK_DBFARM in environment'
    sys.exit(1)

dbname = tstdb

s = process.server(dbname = dbname, stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)

c = process.client('sql', dbname = dbname, stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)

cout, cerr = c.communicate('''\
call master();

call pausemaster();
call pausemaster();

select * from tmp;
insert into tmp values(30,'2 lost update requests');

call resumemaster();
insert into tmp values(35,'resumed logging');
select * from tmp;

call stopmaster();
insert into tmp values(40,'after being stopped');
select * from tmp;

call stopmaster();
call master(); 
''')

sout, serr = s.communicate()

sys.stdout.write(sout)
sys.stdout.write(cout)
sys.stderr.write(serr)
sys.stderr.write(cerr)
