import sys, os, pymonetdb, platform

db = os.getenv("TSTDB")
port = int(os.getenv("MAPIPORT"))

conn1 = pymonetdb.connect(database=db, port=port, autocommit=True, username='monetdb', password='monetdb')
cur1 = conn1.cursor()
try:
    cur1.execute('select cast(1 as hugeint)')
    has_huge = True
except pymonetdb.DatabaseError as e:
    has_huge = False

architecture = platform.architecture()[0]

try:
    cur1.execute('SELECT * from tables OFFSET 2147483647;')
    if architecture == '32bit':
        sys.stderr.write("Exception expected")
except pymonetdb.DatabaseError as e:
    if architecture == '32bit':
        if "Illegal argument" not in str(e):
            sys.stderr.write('Wrong error %s, expected Illegal argument' % (str(e)))
    else:
        raise e

cur1.execute('SELECT * from tables OFFSET 2147483646;')
if cur1.fetchall() != []:
    sys.stderr.write("An empty result set expected")

cur1.close()
conn1.close()
