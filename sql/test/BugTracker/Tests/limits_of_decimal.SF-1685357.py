import sys, os, pymonetdb

db = os.getenv("TSTDB")
port = int(os.getenv("MAPIPORT"))

conn1 = pymonetdb.connect(database=db, port=port, autocommit=True, username='monetdb', password='monetdb')
cur1 = conn1.cursor()
try:
    cur1.execute('select cast(1 as hugeint)')
    has_huge = True
except pymonetdb.DatabaseError as e:
    has_huge = False

if has_huge:
    cur1.execute("""
    start transaction;
    create table tab1 (col1 decimal(20,0) not null);
    rollback;
    """)
    try:
        cur1.execute('create table tab2 (col1 decimal(40,0) not null);')
        sys.stderr.write("Exception expected")
    except pymonetdb.DatabaseError as e:
        if "Decimal(40,0) isn't supported because P=40 > 38 in:" not in str(e):
            sys.stderr.write('Wrong error %s, expected Decimal(40,0) isn\'t supported because P=40 > 38 in:' % (str(e)))
else:
    try:
        cur1.execute('create table tab1 (col1 decimal(20,0) not null);')
        sys.stderr.write("Exception expected")
    except pymonetdb.DatabaseError as e:
        if "Decimal(20,0) isn't supported because P=20 > 18 in:" not in str(e):
            sys.stderr.write('Wrong error %s, expected Decimal(20,0) isn\'t supported because P=20 > 18 in:' % (str(e)))
    try:
        cur1.execute('create table tab2 (col1 decimal(40,0) not null);')
        sys.stderr.write("Exception expected")
    except pymonetdb.DatabaseError as e:
        if "Decimal(40,0) isn't supported because P=40 > 18 in:" not in str(e):
            sys.stderr.write('Wrong error %s, expected Decimal(40,0) isn\'t supported because P=40 > 18 in:' % (str(e)))
cur1.close()
conn1.close()
