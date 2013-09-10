

import monetdb.sql
import os, sys, time

port = int(os.environ['MAPIPORT'])
db = os.environ['TSTDB']
host = os.environ['MAPIHOST']

dbh = monetdb.sql.Connection(port=port,database=db,hostname=host,autocommit=True)

cursor = dbh.cursor();

cursor.execute('select p.*, "location", "count" from storage(), (select value from env() where name = \'gdk_dbpath\') as p where "table"=\'lineitem\';');
res = (cursor.fetchall())
for (dbpath, fn, count) in res:
    f = fn
    fn =  os.path.join(dbpath, 'bat', fn + '.tail');
    print(f, int(os.path.getsize(fn)), count)

