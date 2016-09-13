

import pymonetdb
import os, sys, time

port = int(os.environ['MAPIPORT'])
db = os.environ['TSTDB']
host = os.environ['MAPIHOST']

dbh = pymonetdb.connect(port=port,database=db,hostname=host,autocommit=True)

cursor = dbh.cursor();

cursor.execute('select p.*, "location", "count", "column" from storage(), (select value from env() where name = \'gdk_dbpath\') as p where "table"=\'lineitem\' order by "column"');
res = (cursor.fetchall())
for (dbpath, fn, count, column) in res:
    fn =  os.path.join(dbpath, 'bat', fn + '.tail');
    print(column, int(os.path.getsize(fn)), count)
