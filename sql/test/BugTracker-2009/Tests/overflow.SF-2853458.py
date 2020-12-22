import sys, os, platform, pymonetdb

db = os.getenv("TSTDB")
port = int(os.getenv("MAPIPORT"))

conn1 = pymonetdb.connect(database=db, port=port, autocommit=True, username='monetdb', password='monetdb')
cur1 = conn1.cursor()
running_arch = platform.machine()

try:
    cur1.execute("select cast(power(2,63) as bigint);")
    if running_arch == 'ppc64':
        if cur1.fetchall() != [(9223372036854775807,)]:
            sys.stderr.write('[(9223372036854775807,)] expected\n')
    else:
        sys.stderr.write("Exception expected")
except pymonetdb.DatabaseError as e:
    if running_arch == 'ppc64':
        raise e
    elif "overflow in conversion" not in str(e):
        sys.stderr.write('Wrong error %s, expected overflow in conversion' % (str(e)))
try:
    cur1.execute("select cast(power(2,64) as bigint);")
    sys.stderr.write("Exception expected")
except pymonetdb.DatabaseError as e:
    if "overflow in conversion" not in str(e):
        sys.stderr.write('Wrong error %s, expected overflow in conversion' % (str(e)))

cur1.close()
conn1.close()
