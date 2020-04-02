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
print(cursor2.fetchall())
cursor1.execute('SELECT qaid, value_ FROM tmp.close_d;')
print(cursor1.fetchall())

cursor2.execute('INSERT INTO tmp.close_d VALUES (2,2);')
cursor2.execute('SELECT qaid, value_ FROM tmp.close_d;')
print(cursor2.fetchall())
cursor1.execute('SELECT qaid, value_ FROM tmp.close_d;')
print(cursor1.fetchall())

cursor1.execute('DROP TABLE tmp.close_d;')
try:
    cursor2.execute('SELECT qaid, value_ FROM tmp.close_d;')  # error, tmp.close_d doesn't exist anymore
except pymonetdb.OperationalError as e:
    sys.stderr.write(str(e))

cursor1.close()
cursor2.close()
client1.close()
client2.close()
