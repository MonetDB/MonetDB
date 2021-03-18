import os, sys, pymonetdb


port = int(os.environ['MAPIPORT'])
db = os.environ['TSTDB']

conn1 = pymonetdb.connect(port=port,database=db,autocommit=True,username='monetdb',password='monetdb')
cur1 = conn1.cursor()
cur1.execute("""
start transaction;
create schema s1;
CREATE USER u1 WITH PASSWORD '1' NAME 'u1' SCHEMA s1;
CREATE FUNCTION s1.f1() RETURNS INT BEGIN RETURN 10; END;
CREATE FUNCTION s1.f1(a int) RETURNS INT BEGIN RETURN 10 + a; END;
CREATE FUNCTION s1.f1(a int, b int) RETURNS INT BEGIN RETURN b + a; END;
commit;
""")
cur1.close()
conn1.close()

conn1 = pymonetdb.connect(port=port,database=db,autocommit=True,username='u1',password='1')
cur1 = conn1.cursor()
try:
    cur1.execute('SELECT s1.f1();') # error, not allowed
    sys.stderr.write("Exception expected")
except pymonetdb.DatabaseError as e:
    if "SELECT: insufficient privileges for operator 'f1'" not in str(e):
        sys.stderr.write('Wrong error %s, expected SELECT: insufficient privileges for operator \'f1\'' % (str(e)))
try:
    cur1.execute('SELECT s1.f1(1);') # error, not allowed
    sys.stderr.write("Exception expected")
except pymonetdb.DatabaseError as e:
    if "SELECT: insufficient privileges for unary operator 'f1(tinyint)'" not in str(e):
        sys.stderr.write('Wrong error %s, expected SELECT: insufficient privileges for unary operator \'f1(tinyint)\'' % (str(e)))
try:
    cur1.execute('CALL sys.flush_log();') # error, not allowed
    sys.stderr.write("Exception expected")
except pymonetdb.DatabaseError as e:
    if "SELECT: insufficient privileges for operator 'flush_log'" not in str(e):
        sys.stderr.write('Wrong error %s, expected SELECT: insufficient privileges for operator \'flush_log\'' % (str(e)))
try:
    cur1.execute('SELECT s1.f1(1, 2);') # error, not allowed
    sys.stderr.write("Exception expected")
except pymonetdb.DatabaseError as e:
    if "SELECT: insufficient privileges for binary operator 'f1(tinyint,tinyint)'" not in str(e):
        sys.stderr.write('Wrong error %s, expected SELECT: insufficient privileges for binary operator \'f1(tinyint,tinyint)\'' % (str(e)))
cur1.close()
conn1.close()

conn1 = pymonetdb.connect(port=port,database=db,autocommit=True,username='monetdb',password='monetdb')
cur1 = conn1.cursor()
cur1.execute('GRANT EXECUTE ON FUNCTION s1.f1() TO u1;')
cur1.close()
conn1.close()

conn1 = pymonetdb.connect(port=port,database=db,autocommit=True,username='u1',password='1')
cur1 = conn1.cursor()
cur1.execute('SELECT s1.f1();')
if cur1.fetchall() != [(10,)]:
    sys.stderr.write("[(10,)] expected")
try:
    cur1.execute('SELECT s1.f1(1);') # error, not allowed
    sys.stderr.write("Exception expected")
except pymonetdb.DatabaseError as e:
    if "SELECT: insufficient privileges for unary operator 'f1(tinyint)'" not in str(e):
        sys.stderr.write('Wrong error %s, expected SELECT: insufficient privileges for unary operator \'f1(tinyint)\'' % (str(e)))
cur1.close()
conn1.close()

conn1 = pymonetdb.connect(port=port,database=db,autocommit=True,username='monetdb',password='monetdb')
cur1 = conn1.cursor()
cur1.execute("""
start transaction;
drop user u1;
drop schema s1 cascade;
commit;
""")
cur1.close()
conn1.close()
