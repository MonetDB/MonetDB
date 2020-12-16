import sys, os, pymonetdb
from decimal import Decimal

db = os.getenv("TSTDB")
port = int(os.getenv("MAPIPORT"))

conn1 = pymonetdb.connect(database=db, port=port, autocommit=True, username='monetdb', password='monetdb')
cur1 = conn1.cursor()
try:
    cur1.execute('select cast(1 as hugeint)')
    has_huge = True
except pymonetdb.DatabaseError as e:
    has_huge = False

cur1.execute("""
CREATE TABLE test_num_data (id integer, val numeric(18,10));
INSERT INTO test_num_data VALUES (1, '-0.0'),(2, '-34338492.215397047');
""")

cur1.execute("SELECT * FROM test_num_data;")
if cur1.fetchall() != [(1, Decimal('0E-10')), (2, Decimal('-34338492.2153970470'))]:
    sys.stderr.write('[(1, Decimal(\'0E-10\')), (2, Decimal(\'-34338492.2153970470\'))] expected\n')
try:
    cur1.execute("SELECT t1.id, t2.id, t1.val * t2.val FROM test_num_data t1, test_num_data t2;")
    if has_huge:
        if cur1.fetchall() != [(1, 1, Decimal('0E-20')), (1, 2, Decimal('0E-20')), (2, 1, Decimal('0E-20')), (2, 2, Decimal('1179132047626883.59686213585632020900'))]:
            sys.stderr.write('[(1, 1, Decimal(\'0E-20\')), (1, 2, Decimal(\'0E-20\')), (2, 1, Decimal(\'0E-20\')), (2, 2, Decimal(\'1179132047626883.59686213585632020900\'))] expected\n')
    else:
        sys.stderr.write("Exception expected")
except pymonetdb.DatabaseError as e:
    if has_huge:
        raise e
    elif "overflow in calculation" not in str(e):
        sys.stderr.write('Wrong error %s, expected overflow in calculation' % (str(e)))
try:
    cur1.execute("SELECT t1.id, t2.id, round(t1.val * t2.val, 30) FROM test_num_data t1, test_num_data t2;")
    if has_huge:
        if cur1.fetchall() != [(1, 1, Decimal('0E-20')), (1, 2, Decimal('0E-20')), (2, 1, Decimal('0E-20')), (2, 2, Decimal('1179132047626883.59686213585632020900'))]:
            sys.stderr.write('[(1, 1, Decimal(\'0E-20\')), (1, 2, Decimal(\'0E-20\')), (2, 1, Decimal(\'0E-20\')), (2, 2, Decimal(\'1179132047626883.59686213585632020900\'))] expected\n')
    else:
        sys.stderr.write("Exception expected")
except pymonetdb.DatabaseError as e:
    if has_huge:
        raise e
    elif "overflow in calculation" not in str(e):
        sys.stderr.write('Wrong error %s, expected overflow in calculation' % (str(e)))

# This is a leftover of int128 vs no-int128 from sqlancer07 test. Leave it here just to not create another test
try:
    cur1.execute("SELECT CAST(((24829)+(((0.9767751031140547)*(0.7479400824095245)))) AS DOUBLE) IS NULL;")
    if has_huge:
        if cur1.fetchall() != [(False,)]:
            sys.stderr.write('[(False,)] expected\n')
    else:
        sys.stderr.write("Exception expected")
except pymonetdb.DatabaseError as e:
    if has_huge:
        raise e
    elif "overflow in conversion" not in str(e):
        sys.stderr.write('Wrong error %s, expected overflow in conversion' % (str(e)))

cur1.execute("drop table test_num_data;")
cur1.close()
conn1.close()
