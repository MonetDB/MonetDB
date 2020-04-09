import sys, os, pymonetdb

port = int(os.environ['MAPIPORT'])
db = os.environ['TSTDB']

client1 = pymonetdb.connect(port=port,database=db,autocommit=True)
client2 = pymonetdb.connect(port=port,database=db,autocommit=True)
cursor1 = client1.cursor()
cursor2 = client2.cursor()

cursor1.execute('DECLARE TABLE mytest (a int);')
print(cursor1.execute('SELECT a FROM mytest;'))
try:
    cursor2.execute('SELECT a FROM mytest;')  # error, mytest doesn't exist on the second client
except pymonetdb.OperationalError as e:
    sys.stderr.write(str(e))

client3 = pymonetdb.connect(port=port,database=db,autocommit=True)
cursor3 = client3.cursor()
try:
    cursor3.execute('SELECT a FROM mytest;')  # error, mytest doesn't exist on the third client
except pymonetdb.OperationalError as e:
    sys.stderr.write(str(e))

cursor1.execute('DROP TABLE mytest;')
try:
    cursor1.execute('SELECT a FROM mytest;')  # error, mytest doesn't exist on the first either
except pymonetdb.OperationalError as e:
    sys.stderr.write(str(e))

cursor1.close()
cursor2.close()
cursor3.close()
client1.close()
client2.close()
client3.close()
