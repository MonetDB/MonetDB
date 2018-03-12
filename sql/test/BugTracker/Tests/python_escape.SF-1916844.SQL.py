from __future__ import print_function

import pymonetdb
import os

dbh = pymonetdb.connect(database = os.environ['TSTDB'],
                        port = int(os.environ['MAPIPORT']),
                        hostname = os.environ['MAPIHOST'])

cursor = dbh.cursor()

try:
    cursor.execute('create table python_table (i smallint,s string)')

    s = [0, 'row2']
    try:
        x = cursor.execute("insert into python_table VALUES (%s, %s)", s)
    except:
        print('execute failed with list')
    else:
        print(x)
    s = [1, 'row2']

    try:
        x = cursor.execute("insert into python_table VALUES (%s, %s)", s)
    except:
        print('execute failed with list')
    else:
        print(x)

    s = {'i': 2, 's': 'row1'}
    try:
        x = cursor.execute("insert into python_table VALUES (%(i)s, %(s)s)", s)
    except:
        print('execute failed with dictionary')
    else:
        print(x)

    s = ((3, 'row3'), (4, 'row4'))
    try:
        x = cursor.executemany("insert into python_table VALUES (%s, %s)", s)
    except:
        print('executemany failed with tuple in tuple')
    else:
        print(x)

    s = [(5, 'row5'), (6, 'row6')]
    try:
        x = cursor.executemany("insert into python_table VALUES (%s, %s)", s)
    except:
        print('executemany failed with tuple in list')
    else:
        print(x)

    s = ([7, 'row7'], [8, 'row8'])
    try:
        x = cursor.executemany("insert into python_table VALUES (%s, %s)", s)
    except:
        print('executemany failed with list in tuple')
    else:
        print(x)

    s = [[9, 'row9'], [10, 'row10']]
    try:
        x = cursor.executemany("insert into python_table VALUES (%s, %s)", s)
    except:
        print('executemany failed with list in list')
    else:
        print(x)

    s = [{'i': 9, 's':'row9'}, {'i': 10, 's': 'row10'}]
    try:
        x = cursor.executemany("insert into python_table VALUES (%(i)s, %(s)s)", s)
    except:
        print('executemany failed with dict in list')
    else:
       print(x)
finally:
    cursor.execute('drop table python_table')
