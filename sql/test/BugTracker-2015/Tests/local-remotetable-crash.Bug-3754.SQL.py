import os, pymonetdb

db = os.getenv("TSTDB")
port = int(os.getenv("MAPIPORT"))

client1 = pymonetdb.connect(database=db, port=port, autocommit=True)
cur1 = client1.cursor()
cur1.execute('''
CREATE TABLE t1 (i int);
CREATE REMOTE TABLE rt (LIKE t1) ON 'mapi:monetdb://localhost:{}/{}';
'''.format(os.environ['MAPIPORT'], os.environ['TSTDB']))

try:
    cur1.execute('SELECT * from rt;')
    print('Exception expected')
except Exception as e:
    if 'Remote tables not supported under remote connections' not in str(e):
        print(str(e))

cur1.close()
client1.close()

client2 = pymonetdb.connect(database=db, port=port, autocommit=True)
cur2 = client2.cursor()
cur2.execute('''
DROP TABLE rt;
DROP TABLE t1;
''')
cur2.close()
client2.close()
