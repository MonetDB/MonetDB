import pymonetdb
import os

port = int(os.environ['MAPIPORT'])
db = os.environ['TSTDB']

dbh = pymonetdb.connect(port=port,database=db,autocommit=True)
cursor = dbh.cursor()
cursor.execute("explain select count(*) from sys.tables")
cursor.fetchone()
