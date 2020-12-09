import os, sys, pymonetdb


client1 = pymonetdb.connect(port=int(os.getenv('MAPIPORT')), database=os.getenv('TSTDB'))
cur1 = client1.cursor()
f = open(os.path.join(os.getenv('TSTSRCBASE'), os.getenv('TSTDIR'), 'Tests/null-byte-hang.sql'), 'r')
q = f.read()
f.close()
try:
    cur1.execute(q)
    sys.stderr.write('Error expected')
except Exception as ex:
    if 'NULL byte in string' not in str(ex):
        sys.stderr.write('Error: NULL byte in string expected')

cur1.close()
client1.close()
