import monetdb.sql, os

dbh = monetdb.sql.Connection(port=int(os.getenv('MAPIPORT')),hostname=os.getenv('MAPIHOST'),database=os.getenv('TSTDB'))
cursor = dbh.cursor();
cursor.execute('create table bug3439 (ts timestamp)')
cursor.execute("insert into bug3439 values ('2014-04-24 17:12:12.415')")
cursor.execute('select * from bug3439')
print cursor.fetchall()
cursor.execute('drop table bug3439')
