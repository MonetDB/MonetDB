import pymonetdb, sys, os

port = int(os.environ['MAPIPORT'])
db = os.environ['TSTDB']

cli = pymonetdb.connect(port=port,database=db,autocommit=True, username='monetdb',password='monetdb')
cur = cli.cursor()
cur.execute("CREATE USER \"psm\" WITH PASSWORD 'psm' NAME 'PSM' SCHEMA \"sys\";")
cur.close()
cli.close()

cli = pymonetdb.connect(port=port,database=db,autocommit=True, username='psm',password='psm')
cur = cli.cursor()
try:
    cur.execute("explain select * from storagemodel();")
    sys.stderr.write("This query: explain select * from storagemodel(); was supposed to fail")
except pymonetdb.DatabaseError as e:
    if 'no such table returning function \'storagemodel\'()' not in str(e):
        sys.stderr.write(str(e))
try:
    cur.execute("select * from storagemodel();")
    sys.stderr.write("This query: select * from storagemodel(); was supposed to fail")
except pymonetdb.DatabaseError as e:
    if 'no such table returning function \'storagemodel\'()' not in str(e):
        sys.stderr.write(str(e))
try:
    cur.execute("select * from storagemodel();")
    sys.stderr.write("This query: select * from storagemodel(); was supposed to fail")
except pymonetdb.DatabaseError as e:
    if 'no such table returning function \'storagemodel\'()' not in str(e):
        sys.stderr.write(str(e))
cur.close()
cli.close()

cli = pymonetdb.connect(port=port,database=db,autocommit=True, username='monetdb',password='monetdb')
cur = cli.cursor()
cur.execute("DROP USER \"psm\";")
cur.close()
cli.close()
