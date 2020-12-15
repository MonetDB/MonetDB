import pymonetdb, sys, os

port = int(os.environ['MAPIPORT'])
db = os.environ['TSTDB']

cli = pymonetdb.connect(port=port,database=db,autocommit=True)
cur = cli.cursor()
q = []
q.append(("start transaction; create table bug3261 (probeid int, markername varchar(64));\n"
            "copy %d records into bug3261 from stdin using delimiters "
            "E'\\t',E'\\n','' null as 'null';\n") % (1455 * 3916))
for i in range(1,1456):
    v = 'rmm%d' % i
    for j in range(3916):
        q.append('%d\t%s\n' % (j, v))
q.append("commit;")
cur.execute(''.join(q))
cur.close()
cli.close()

cli = pymonetdb.connect(port=port,database=db,autocommit=True)
cur = cli.cursor()
cur.execute('select * from bug3261 where probeid = 1234 limit 10;')
if cur.fetchall() != [(1234,"rmm1"),(1234,"rmm2"),(1234,"rmm3"),(1234,"rmm4"),(1234,"rmm5"),(1234,"rmm6"),(1234,"rmm7"),(1234,"rmm8"),(1234,"rmm9"),(1234,"rmm10")]:
    sys.stderr.write('Expected [(1234,"rmm1"),(1234,"rmm2"),(1234,"rmm3"),(1234,"rmm4"),(1234,"rmm5"),(1234,"rmm6"),(1234,"rmm7"),(1234,"rmm8"),(1234,"rmm9"),(1234,"rmm10")]')
cur.close()
cli.close()

cli = pymonetdb.connect(port=port,database=db,autocommit=True)
cur = cli.cursor()
cur.execute('drop table bug3261;')
cur.close()
cli.close()

