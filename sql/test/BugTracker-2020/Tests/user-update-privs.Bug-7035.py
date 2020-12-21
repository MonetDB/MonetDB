import os, sys, pymonetdb


port = int(os.environ['MAPIPORT'])
db = os.environ['TSTDB']

conn1 = pymonetdb.connect(port=port,database=db,autocommit=True,username='monetdb',password='monetdb')
cur1 = conn1.cursor()
cur1.execute("""
start transaction;
create table t (a int, b int);
create user usr with password 'usr' name 'usr' schema sys;
grant select(a) on t to usr;
grant update (b) on t to usr;
grant delete on t to usr;
commit;
""")
cur1.close()
conn1.close()

conn1 = pymonetdb.connect(port=port,database=db,autocommit=True,username='usr',password='usr')
cur1 = conn1.cursor()
cur1.execute('select a from t;')
if cur1.fetchall() != []:
    sys.stderr.write("[] expected")
if cur1.execute('update t set b = 23;') != 0:
     sys.stderr.write("0 rows affected expected")
if cur1.execute('update t set b = 32 where a = 6;') != 0:
     sys.stderr.write("0 rows affected expected")
if cur1.execute('update t set b = a;') != 0:
     sys.stderr.write("0 rows affected expected")
if cur1.execute('delete from t;') != 0:
     sys.stderr.write("0 rows affected expected")
if cur1.execute('delete from t where a = 2;') != 0:
     sys.stderr.write("0 rows affected expected")
try:
    cur1.execute('update t set b = b;') # error, not allowed
    sys.stderr.write("Exception expected")
except pymonetdb.DatabaseError as e:
    if "SELECT: identifier 'b' unknown" not in str(e):
        sys.stderr.write('Wrong error %s, expected SELECT: identifier \'b\' unknown' % (str(e)))
try:
    cur1.execute('update t set b = 1 where b = 5;') # error, not allowed
    sys.stderr.write("Exception expected")
except pymonetdb.DatabaseError as e:
    if "SELECT: identifier 'b' unknown" not in str(e):
        sys.stderr.write('Wrong error %s, expected SELECT: identifier \'b\' unknown' % (str(e)))
try:
    cur1.execute('delete from t where b = 5;') # error, not allowed
    sys.stderr.write("Exception expected")
except pymonetdb.DatabaseError as e:
    if "SELECT: identifier 'b' unknown" not in str(e):
        sys.stderr.write('Wrong error %s, expected SELECT: identifier \'b\' unknown' % (str(e)))

cur1.close()
conn1.close()

conn1 = pymonetdb.connect(port=port,database=db,autocommit=True,username='monetdb',password='monetdb')
cur1 = conn1.cursor()
cur1.execute("""
start transaction;
drop user usr;
drop table t;
commit;
""")
cur1.close()
conn1.close()
