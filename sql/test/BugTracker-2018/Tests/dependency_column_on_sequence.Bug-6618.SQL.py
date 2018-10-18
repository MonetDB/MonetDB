from __future__ import print_function

try:
    from MonetDBtesting import process
except ImportError:
    import process

import sys, time, pymonetdb, os

def connect(autocommit):
    return pymonetdb.connect(database = os.getenv('TSTDB'),
                             hostname = '127.0.0.1',
                             port = int(os.getenv('MAPIPORT')),
                             username = 'monetdb',
                             password = 'monetdb',
                             autocommit = autocommit)

def query(conn, sql):
    cur = conn.cursor()
    cur.execute(sql)
    r = cur.fetchall()
    cur.close()
    return r

def run(conn, sql, echo=True):
    if echo:
        print(sql)
    r = conn.execute(sql)

# Test automatically created sequence:
c1 = connect(True)
run(c1, 'create table t (i int auto_increment, val int)')
run(c1, 'insert into t(val) values (10), (20)')

# try to find the name of the automatically created sequence
# this is a workaround, since the dependency is not registered in the
#  "dependencies" table
res = query(c1, 'select name from sequences where id = (select max(id) from sequences)')
if len(res) != 1 :
    print("Invalid results: " + res)
    exit(1)
elif len(res[0]) != 1 :
    print("Invalid results: " + res[0])
    exit(1)
seqname = res[0][0]
try:
  run(c1, 'drop sequence ' + seqname, False)
  run(c1, 'insert into t(val) values (30), (40)')
except Exception, e:
  print(e, file=sys.stderr)
print(query(c1, 'select * from t'))
run(c1, 'drop table t')


# Test explicitly created sequence:
run(c1, 'create sequence myseq as int');
run(c1, 'create table t2 (i int default next value for myseq, val int)')
run(c1, 'insert into t2(val) values (10), (20)')
try:
  run(c1, 'drop sequence myseq')
  run(c1, 'insert into t2(val) values (30), (40)')
except Exception, e:
  print(e, file=sys.stderr)
print(query(c1, 'select * from t2'))
run(c1, 'drop table t2')

# clean up
c1.close()
