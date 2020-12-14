import sys, pymonetdb, os

# Test automatically created sequence:
db = os.getenv("TSTDB")
port = int(os.getenv("MAPIPORT"))
c1 = pymonetdb.connect(database=db, port=port, autocommit=True, username='monetdb', password='monetdb')

cur = c1.cursor()
cur.execute("""
create table t (i int auto_increment, val int);
insert into t(val) values (10), (20);
""")

# try to find the name of the automatically created sequence
# this is a workaround, since the dependency is not registered in the
#  "dependencies" table
cur.execute('select name from sequences where id = (select max(id) from sequences)')
res = cur.fetchall()
if len(res) != 1:
    sys.stderr.write("Invalid results: " + res)
elif len(res[0]) != 1:
    sys.stderr.write("Invalid results: " + res[0])

seqname = res[0][0]
try:
    cur.execute('drop sequence ' + seqname)
    sys.stderr.write("Exception expected")
except pymonetdb.DatabaseError as e:
    if "DROP SEQUENCE: unable to drop sequence" not in str(e):
        sys.stderr.write("Error: DROP SEQUENCE: unable to drop sequence")

cur.execute('select * from t')
if cur.fetchall() != [(1, 10), (2, 20)]:
    sys.stderr.write('Expected result: [(1, 10), (2, 20)]')
cur.execute('drop table t')

# Test explicitly created sequence:
cur.execute("""
create sequence myseq as int;
create table t2 (i int default next value for myseq, val int);
insert into t2(val) values (10), (20);
""")

try:
    cur.execute('drop sequence myseq')
    sys.stderr.write("Exception expected")
except pymonetdb.DatabaseError as e:
    if "DROP SEQUENCE: unable to drop sequence" not in str(e):
        sys.stderr.write("Error: DROP SEQUENCE: unable to drop sequence")

cur.execute('select * from t2')
if cur.fetchall() != [(1, 10), (2, 20)]:
    sys.stderr.write('Expected result: [(1, 10), (2, 20)]')
cur.execute("""
drop table t2;
drop sequence myseq;
""")

# clean up
cur.close()
c1.close()
