import MonetSQLdb
import os

dbh = MonetSQLdb.Connection(dbname = os.environ['TSTDB'],
                            port = int(os.environ['MAPIPORT']))

cursor = dbh.cursor()

try:
    cursor.execute('create table python_table (i smallint,s string)')

    s = (0, 'row1')
    try:
        x = cursor.execute("insert into python_table VALUES (%s, %s)", s)
    except:
        print 'execute failed with tuple'
    else:
        print x

    s = [1, 'row2']
    try:
        x = cursor.execute("insert into python_table VALUES (%s, %s)", s)
    except:
        print 'execute failed with list'
    else:
        print x

    s = ((2, 'row3'), (3, 'row4'))
    try:
        x = cursor.executemany("insert into python_table VALUES (%s, %s)", s)
    except:
        print 'executemany failed with tuple in tuple'
    else:
        print x

    s = [(4, 'row5'), (5, 'row6')]
    try:
        x = cursor.executemany("insert into python_table VALUES (%s, %s)", s)
    except:
        print 'executemany failed with tuple in list'
    else:
        print x

    s = ([6, 'row7'], [7, 'row8'])
    try:
        x = cursor.executemany("insert into python_table VALUES (%s, %s)", s)
    except:
        print 'executemany failed with list in tuple'
    else:
        print x

    s = [[8, 'row9'], [9, 'row10']]
    try:
        x = cursor.executemany("insert into python_table VALUES (%s, %s)", s)
    except:
        print 'executemany failed with list in list'
    else:
        print x
finally:
    cursor.execute('drop table python_table')
