import pymonetdb
import os

users = ['user1', 'user2', 'user3', 'user4', 'user5', 'user6', 'user7']
try:
    mdbdbh = pymonetdb.connect(database = os.environ['TSTDB'], port = int(os.environ['MAPIPORT']), hostname = os.environ['MAPIHOST'], autocommit=True)
    #mdbdbh = pymonetdb.connect(database = 'usrstat', autocommit=True)
    mdbcursor = mdbdbh.cursor()
    rowcnt = mdbcursor.execute('create procedure sleep(i int) external name alarm.sleep')
    rowcnt = mdbcursor.execute('call sys.sleep(1000)')
    rowcnt = mdbcursor.execute('select querycount from sys.user_statistics() where username = \'monetdb\'')
    mdbqrycnt = mdbcursor.fetchone()[0]

    # create and run some user queries to populate the user_statistics table
    for usr in users:
        mdbcursor.execute('create user "{usr}" with password \'{usr}\' name \'{usr}\' schema "sys"'.format(usr=usr))
        mdbcursor.execute('grant all on procedure sys.sleep to {usr}'.format(usr=usr))

        try:
            usrdbh = pymonetdb.connect(database = os.environ['TSTDB'], port = int(os.environ['MAPIPORT']), hostname = os.environ['MAPIHOST'], username=usr, password=usr, autocommit=True)
            #usrdbh = pymonetdb.connect(database = 'usrstat', username=usr, password=usr, autocommit=True)
            usrcursor = usrdbh.cursor()
            usrcursor.execute('select current_user as myname')
            usrcursor.execute('call sys.sleep(1000)')
        except pymonetdb.exceptions.Error as e:
            print(usr + ' query failed')
            print(e)
        finally:
            usrdbh.close()

    # now check user_statistics again
    rowcnt = mdbcursor.execute('select username, querycount, maxquery from sys.user_statistics()')
    records = mdbcursor.fetchall()
    print("User statistics after: {cnt}".format(cnt=rowcnt))
    for row in records:
        if row[0] == 'monetdb':
            print((row[0], int(row[1]) - mdbqrycnt, row[2]))
        else:
            print(row)
except pymonetdb.exceptions.Error as e:
    print(e)
finally:
    # clean up the created users and don't stop by an error
    for usr in users:
        try:
            mdbcursor.execute('drop user {usr}'.format(usr=usr))
        except pymonetdb.exceptions.Error as e:
            print(e)
    try:
        mdbcursor.execute('drop procedure sleep')
    except pymonetdb.exceptions.Error as e:
        print(e)
    mdbdbh.close()




#import pymonetdb
#import os
#dbh = pymonetdb.connect(database = 'usrstat')
#cur = dbh.cursor()
#cur.execute('create user "user1" with password \'user1\' name \'user1\' schema "sys"')
#dbh = pymonetdb.connect(database = 'usrstat', username='user1', password='user1')
#cur = dbh.cursor()
#cur.execute('select current_user')
