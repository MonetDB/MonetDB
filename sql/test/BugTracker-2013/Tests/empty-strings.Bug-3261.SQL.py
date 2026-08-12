from MonetDBtesting import tpymonetdb as pymonetdb
import sys, os

port = int(os.environ['MAPIPORT'])
db = os.environ['TSTDB']

cli = pymonetdb.connect(port=port,database=db,autocommit=True)
cur = cli.cursor()
q = f"create table bug3261 as (select (value % 1456) + 1 as probeid, 'rmm' || cast((value / 1456) + 1 as string) as markername from sys.generate_series(0,{1456 * 3916}));\n"
cur.execute(q)
cur.close()
cli.close()

cli = pymonetdb.connect(port=port,database=db,autocommit=True)
cur = cli.cursor()
cur.execute("select * from bug3261 where probeid = 1234 and (markername like 'rmm_' or markername = 'rmm10') limit 10;")
rcv = cur.fetchall()
exp = [(1234,"rmm1"),(1234,"rmm2"),(1234,"rmm3"),(1234,"rmm4"),(1234,"rmm5"),(1234,"rmm6"),(1234,"rmm7"),(1234,"rmm8"),(1234,"rmm9"),(1234,"rmm10")]
if sorted(rcv) != sorted(exp):
    sys.stderr.write('Expected {}\n'.format(exp))
    sys.stderr.write('Received {}\n'.format(rcv))
cur.close()
cli.close()

cli = pymonetdb.connect(port=port,database=db,autocommit=True)
cur = cli.cursor()
cur.execute('drop table bug3261;')
cur.close()
cli.close()
