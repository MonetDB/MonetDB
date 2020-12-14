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

try:
    cur1.execute('select                             10000000                *          100000 * 11.51                +          51.097 *          100000;')
    if has_huge:
        if cur1.fetchall() != [(11510005109700,)]:
            sys.stderr.write('[(11510005109700,)] expected')
    else:
        sys.stderr.write("Exception expected")
except pymonetdb.DatabaseError as e:
    if not has_huge:
        if "overflow in calculation" not in str(e):
            sys.stderr.write('Wrong error %s, expected overflow in calculation' % (str(e)))
    else:
        raise e
try:
    cur1.execute('select          convert(1000000000000000000 , decimal(20)) * 100000000000000 * 11.51                +          51.097 * 100000000000000;')
    if has_huge:
        if cur1.fetchall() != [(11510005109700,)]:
            sys.stderr.write('[(11510005109700,)] expected')
    else:
        sys.stderr.write("Exception expected")
except pymonetdb.DatabaseError as e:
    if has_huge:
        if "overflow in calculation" not in str(e):
            sys.stderr.write('Wrong error %s, expected overflow in calculation' % (str(e)))
    else:
        if "Decimal of 20 digits are not supported in" not in str(e):
            sys.stderr.write('Wrong error %s, expected Decimal of 20 digits are not supported in' % (str(e)))
cur1.execute('select convert (                   10000000                *          100000 * 11.51 , decimal(15)) + convert (51.097 *          100000 , decimal(15));')
if cur1.fetchall() != [(11510005109700,)]:
    sys.stderr.write('[(11510005109700,)] expected')
try:
    cur1.execute('select convert (convert(1000000000000000000 , decimal(20)) * 100000000000000 * 11.51 , decimal(35)) + convert (51.097 * 100000000000000 , decimal(35));')
    if has_huge:
        if cur1.fetchall() != [(1151000000000000005109700000000000,)]:
            sys.stderr.write('[(1151000000000000005109700000000000,)] expected')
    else:
        sys.stderr.write("Exception expected")
except pymonetdb.DatabaseError as e:
    if not has_huge:
        if "Decimal of 20 digits are not supported in" not in str(e):
            sys.stderr.write('Wrong error %s, expected Decimal of 20 digits are not supported in' % (str(e)))
    else:
        raise e

cur1.close()
conn1.close()
