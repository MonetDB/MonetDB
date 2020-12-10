import os, sys, pymonetdb


port = int(os.environ['MAPIPORT'])
db = os.environ['TSTDB']

conn1 = pymonetdb.connect(port=port,database=db,autocommit=True,username='monetdb',password='monetdb')
cur1 = conn1.cursor()
cur1.execute("""
start transaction;
create schema foo;
create user u1 with password '1' name 'u1' schema foo;
create user u2 with password '2' name 'u2' schema foo;
create table foo.tab1 (col1 int, col2 int);
create table foo.tab2 (col1 int, col2 int);
insert into foo.tab1 values (1, 1);
insert into foo.tab2 values (2, 2);
create view foo.v1(col1,col2) as (select col1, col2 from foo.tab1);
create view foo.v2(col1,col2) as (select col1, col2 from foo.tab2);
commit;
""")
cur1.close()
conn1.close()

conn1 = pymonetdb.connect(port=port,database=db,autocommit=True,username='u1',password='1')
cur1 = conn1.cursor()
try:
    cur1.execute('SELECT "col1" FROM "foo"."v1";') # error, not allowed
    sys.stderr.write("Exception expected")
except pymonetdb.DatabaseError as e:
    if "SELECT: access denied for u1 to view 'foo.v1'" not in str(e):
         sys.stderr.write('Wrong error %s, expected SELECT: access denied for u1 to view \'foo.v1\'' % (str(e)))
try:
    cur1.execute('SELECT "col2" FROM "foo"."v1";') # error, not allowed
    sys.stderr.write("Exception expected")
except pymonetdb.DatabaseError as e:
    if "SELECT: access denied for u1 to view 'foo.v1'" not in str(e):
        sys.stderr.write('Wrong error %s, expected SELECT: access denied for u1 to view \'foo.v1\'' % (str(e)))
try:
    cur1.execute('SELECT "col1" FROM "foo"."tab1";') # error, not allowed
    sys.stderr.write("Exception expected")
except pymonetdb.DatabaseError as e:
    if "SELECT: access denied for u1 to table 'foo.tab1'" not in str(e):
        sys.stderr.write('Wrong error %s, expected SELECT: access denied for u1 to table \'foo.tab1\'' % (str(e)))
cur1.close()
conn1.close()

conn1 = pymonetdb.connect(port=port,database=db,autocommit=True,username='monetdb',password='monetdb')
cur1 = conn1.cursor()
cur1.execute("""
grant select ("col1") ON "foo"."v1" TO u1;
""")
cur1.close()
conn1.close()

conn1 = pymonetdb.connect(port=port,database=db,autocommit=True,username='u1',password='1')
cur1 = conn1.cursor()
cur1.execute('SELECT "col1" FROM "foo"."v1";')
if cur1.fetchall() != [(1, )]:
    sys.stderr.write("[(1, )] expected")
try:
    cur1.execute('SELECT "col2" FROM "foo"."v1";') # error, not allowed
    sys.stderr.write("Exception expected")
except pymonetdb.DatabaseError as e:
    if "SELECT: identifier 'col2' unknown" not in str(e):
        sys.stderr.write('Wrong error %s, expected "SELECT: identifier \'col2\' unknown' % (str(e)))
try:
    cur1.execute('SELECT "col1" FROM "foo"."tab1";') # error, not allowed
    sys.stderr.write("Exception expected")
except pymonetdb.DatabaseError as e:
    if "SELECT: access denied for u1 to table 'foo.tab1'" not in str(e):
        sys.stderr.write('Wrong error %s, expected SELECT: access denied for u1 to table \'foo.tab1\'' % (str(e)))
cur1.close()
conn1.close()

conn1 = pymonetdb.connect(port=port,database=db,autocommit=True,username='monetdb',password='monetdb')
cur1 = conn1.cursor()
cur1.execute("""
start transaction;
drop user u1;
drop user u2;
drop schema foo cascade;
commit;
""")
cur1.close()
conn1.close()
