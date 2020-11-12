import sys, os, pymonetdb

port = int(os.environ['MAPIPORT'])
db = os.environ['TSTDB']

client1 = pymonetdb.connect(port=port,database=db,autocommit=True)
client2 = pymonetdb.connect(port=port,database=db,autocommit=True)
cursor1 = client1.cursor()
cursor2 = client2.cursor()

cursor1.execute('CREATE GLOBAL TEMPORARY TABLE close_d (qaid int, value_ float) ON COMMIT PRESERVE ROWS;')

cursor1.execute('INSERT INTO tmp.close_d VALUES (1,1);')
cursor2.execute('SELECT qaid, value_ FROM tmp.close_d;')
if cursor2.fetchall() != []:
    sys.stderr.write("No rows should have been fetched")
cursor1.execute('SELECT qaid, value_ FROM tmp.close_d;')
if cursor1.fetchall() != [(1, 1.0)]:
    sys.stderr.write("Just row (1, 1.0) expected")

cursor2.execute('INSERT INTO tmp.close_d VALUES (2,2);')
cursor2.execute('SELECT qaid, value_ FROM tmp.close_d;')
if cursor2.fetchall() != [(2, 2.0)]:
    sys.stderr.write("Just row (2, 2.0) expected")

cursor1.execute('SELECT qaid, value_ FROM tmp.close_d;')
if cursor1.fetchall() != [(1, 1.0)]:
    sys.stderr.write("Just row (1, 1.0) expected")

cursor1.execute('DROP TABLE tmp.close_d;')
try:
    cursor2.execute('SELECT qaid, value_ FROM tmp.close_d;')  # error, tmp.close_d doesn't exist anymore
except pymonetdb.OperationalError as e:
    if "no such table 'close_d'" not in str(e):
        sys.stderr.write(str(e))

cursor1.close()
cursor2.close()
client1.close()
client2.close()
