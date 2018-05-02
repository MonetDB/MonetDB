from __future__ import print_function

import pymonetdb, os

dbh = pymonetdb.connect(port=int(os.getenv('MAPIPORT')),hostname=os.getenv('MAPIHOST'),database=os.getenv('TSTDB'))
cursor = dbh.cursor();

cursor.execute('create table bug3439 (ts timestamp)')
cursor.execute("insert into bug3439 values ('2014-04-24 17:12:12.415')")
cursor.execute('select * from bug3439')
print(cursor.fetchall())
cursor.execute('drop table bug3439')

cursor.execute('create table bug3439 (ts timestamp with time zone)')
cursor.execute("insert into bug3439 values ('2014-04-24 17:12:12.415 -02:00')")
cursor.execute('select * from bug3439')
print(cursor.fetchall())
cursor.execute('drop table bug3439')

cursor.execute('create table bug3439 (dt date)')
cursor.execute("insert into bug3439 values ('2014-04-24')")
cursor.execute('select * from bug3439')
print(cursor.fetchall())
cursor.execute('drop table bug3439')

cursor.execute('create table bug3439 (ts time)')
cursor.execute("insert into bug3439 values ('17:12:12.415')")
cursor.execute('select * from bug3439')
print(cursor.fetchall())
cursor.execute('drop table bug3439')

cursor.execute('create table bug3439 (ts time with time zone)')
cursor.execute("insert into bug3439 values ('17:12:12.415 -02:00')")
cursor.execute('select * from bug3439')
print(cursor.fetchall())
cursor.execute('drop table bug3439')
