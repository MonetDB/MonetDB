###
# Check that an ordinary user can see queries in all its sessions
###
from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as mdb:
    mdb.connect(username="monetdb", password="monetdb")
    mdb.execute('create role r1;').assertSucceeded()
    mdb.execute('create schema s1 authorization r1;').assertSucceeded()
    mdb.execute('create user u1 with password \'u1\' name \'u1\' schema s1;').assertSucceeded()
    mdb.execute('grant r1 to u1;').assertSucceeded()

    # Let the user establish several connections to the server
    with SQLTestCase() as tc1:
        tc1.connect(username="u1", password="u1")

        with SQLTestCase() as tc2:
            tc2.connect(username="u1", password="u1")

            # NB, we only have 4-1 slots in sys.queue to use because of the
            # SingleServer config in this test
            tc1.execute('select \'u1 session_1\';').assertSucceeded()\
                    .assertDataResultMatch([("u1 session_1",)])
            tc2.execute('select \'u1 session_2\';').assertSucceeded()\
                    .assertDataResultMatch([("u1 session_2",)])

            # Check that the sys.queue() output of each user contains queries from both
            # connections
            tc1.execute('select username, sessionid from sys.queue() group by username, sessionid order by sessionid;')\
                    .assertSucceeded().assertDataResultMatch([('u1', 1), ('u1', 2)])
            tc2.execute('select username, sessionid from sys.queue() group by username, sessionid order by sessionid;')\
                    .assertSucceeded().assertDataResultMatch([('u1', 1), ('u1', 2)])

    mdb.execute('drop user u1').assertSucceeded()
    mdb.execute('drop role r1').assertSucceeded()
    mdb.execute('drop schema s1').assertSucceeded()
