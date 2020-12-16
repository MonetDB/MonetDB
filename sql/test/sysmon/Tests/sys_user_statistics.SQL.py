###
# Check that the sys.user_statistics() function works, i.e. it correctly logs
#   new users and their query count and maxquery
###
from MonetDBtesting.sqltest import SQLTestCase

users = ['user1', 'user2', 'user3', 'user4']
SLEEP_TIME = "200"

with SQLTestCase() as mdb:
    mdb.connect(username="monetdb", password="monetdb")
    # we use the sleep() function to acquire determinable maxquery
    mdb.execute('create procedure sleep(i int) external name alarm.sleep;').assertSucceeded()

    # create and run some user queries to populate the user_statistics table
    for u in users:
        mdb.execute('create user {u} with password \'{u}\' name \'{u}\' schema sys'.format(u=u)).assertSucceeded()
        mdb.execute('grant all on procedure sys.sleep to {u}'.format(u=u)).assertSucceeded()

        with SQLTestCase() as usr:
            usr.connect(username=u, password=u)
            usr.execute('select current_user as myname').assertSucceeded()
            usr.execute('call sys.sleep('+SLEEP_TIME+')').assertSucceeded()

    # now check user_statistics again
    rowcnt = mdb.execute('select username, querycount, maxquery from sys.user_statistics() where username like \'user%\' order by username;').assertSucceeded().\
        assertDataResultMatch([
            ('user1', 2, 'call sys.sleep('+SLEEP_TIME+')\n;'),
            ('user2', 2, 'call sys.sleep('+SLEEP_TIME+')\n;'),
            ('user3', 2, 'call sys.sleep('+SLEEP_TIME+')\n;'),
            ('user4', 2, 'call sys.sleep('+SLEEP_TIME+')\n;')
            ])
    for u in users:
        mdb.execute('drop user {u};'.format(u=u)).assertSucceeded()
    mdb.execute('drop procedure sleep;').assertSucceeded()

