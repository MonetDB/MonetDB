###
# Check that an ordinary user can see queries in all its sessions
###
import pymonetdb
import os

DB = os.environ['TSTDB']
PORT = int(os.environ['MAPIPORT'])
HOST = os.environ['MAPIHOST']
USR = 'u1'
PSWD = 'u1'

try:
    mdbdbh = pymonetdb.connect(database=DB, port=PORT, hostname=HOST, autocommit=True)
    mdbcursor = mdbdbh.cursor()
    mdbcursor.execute('create role r1;')
    mdbcursor.execute('create schema s1 authorization r1;')
    mdbcursor.execute('create user u1 with password \'u1\' name \'u1\' schema s1;')
    mdbcursor.execute('grant r1 to u1;')

    # Let the user establish several connections to the server
    usrdbh1 = pymonetdb.connect(database=DB, port=PORT, hostname=HOST,
                            username=USR, password=PSWD, autocommit=True)
    usrcursor1 = usrdbh1.cursor()

    usrdbh2 = pymonetdb.connect(database=DB, port=PORT, hostname=HOST,
                            username=USR, password=PSWD, autocommit=True)
    usrcursor2 = usrdbh2.cursor()

    # NB, we only have 4-1 slots in sys.queue to use because of the
    # SingleServer config in this test
    usrcursor1.execute('select \'u1 session_1\';')
    usrcursor2.execute('select \'u1 session_2\';')

    # Check that the sys.queue() output of each user contains queries from both
    # connections
    usrcursor1.execute('select username, sessionid  from sys.queue() group by username, sessionid order by sessionid;')
    if usrcursor1.fetchall() != [('u1', 1), ('u1', 2)]:
        print('Expected: [(\'u1\', 1), (\'u1\', 2)]')
    usrcursor2.execute('select username, sessionid from sys.queue() group by username, sessionid order by sessionid;')
    if usrcursor2.fetchall() != [('u1', 1), ('u1', 2)]:
        print('Expected: [(\'u1\', 1), (\'u1\', 2)]')
except pymonetdb.exceptions.Error as e:
    print(e)
finally:
    # clean up and don't stop by an error
    try:
        if usrdbh1:
            usrdbh1.close()
        if usrdbh2:
            usrdbh2.close()
        mdbcursor.execute('drop user u1')
        mdbcursor.execute('drop role r1')
        mdbcursor.execute('drop schema s1')
        mdbdbh.close()
    except pymonetdb.exceptions.Error as e:
        print(e)

