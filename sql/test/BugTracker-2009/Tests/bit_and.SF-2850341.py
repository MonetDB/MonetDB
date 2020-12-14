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

cur1.execute('select bit_and(3749090034127126942, -1);')
if cur1.fetchall() != [(3749090034127126942,)]:
    sys.stderr.write('[(3749090034127126942,)] expected')
cur1.execute('select bit_and(3749090034127126942, 0x7fffffffffffffff);')
if cur1.fetchall() != [(3749090034127126942,)]:
    sys.stderr.write('[(3749090034127126942,)] expected')
if has_huge:
    cur1.execute('select bit_and(3749090034127126942, 0xffffffffffffffff);')
    if cur1.fetchall() != [(3749090034127126942,)]:
        sys.stderr.write('[(3749090034127126942,)] expected')
