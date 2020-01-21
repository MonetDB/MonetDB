# Our task is as follows.
#
# We create a new database.
# We insert some data and commit it.
# We insert some more data and do not commit it.
# On another connection, we insert even more uncommitted data.
# Then we call hot_snapshot.
# We kill the server, delete the db directory and untar the file.
# We start a server on the untarred db dir and check the data.
# The committed data should exist in the snapshot.
# The uncommitted data should not.


from __future__ import print_function

try:
    from MonetDBtesting import process
except ImportError:
    import process

import os
import shutil
import socket
import time
import tarfile

import pymonetdb

dbfarm = os.getenv('GDK_DBFARM')
tstdb = os.getenv('TSTDB')

assert dbfarm
assert tstdb

mydb = tstdb + '_snap'
mydbdir = os.path.join(dbfarm, mydb)
tarname = os.path.join(dbfarm, mydb + '.tar')

def main():
    server = None
    cleanup_afterward = False;  # we'll set it to true once the tests have succeeded
    try:
        # clean up remainder of earlier run
        if os.path.exists(mydbdir):
            shutil.rmtree(mydbdir)
        if os.path.exists(tarname):
            os.remove(tarname)

        # figure out a free port number
        s = socket.socket()
        s.bind(('0.0.0.0', 0))
        mapi_port = s.getsockname()[1]
        s.close()
        s = None

        # start the server
        server = process.server(dbname=mydb, mapiport = mapi_port, stdin=process.PIPE)
        time.sleep(2)

        # connection 1 creates, inserts, commits and inserts uncommitted
        conn1 = pymonetdb.connect(
            database=server.dbname, hostname='localhost',
            port=mapi_port,
            username="monetdb", password="monetdb",
            autocommit=False
        )
        cur1 = conn1.cursor()
        cur1.execute("create table foo(t varchar(40))")
        cur1.execute("insert into foo values ('committed1')")
        conn1.commit()
        cur1.execute("insert into foo values ('uncommitted1')")

        # connection 2 inserts some more uncommitted
        conn2 = pymonetdb.connect(
            database=server.dbname, hostname='localhost',
            port=mapi_port,
            username="monetdb", password="monetdb",
            autocommit=False
        )
        cur2 = conn2.cursor()
        cur2.execute("insert into foo values ('uncommitted2')")

        # then conn1 creates the snapshot
        cur1.execute("call sys.hot_snapshot(%(tarname)s)", dict(tarname=tarname))

        # we shut down the server and delete the dbdir
        cur1.close()
        conn1.close()
        cur2.close()
        conn2.close()
        server.terminate()
        time.sleep(1)
        shutil.rmtree(mydbdir)

        # and extract the tarname
        tar = tarfile.open(tarname)
        tar.extractall(dbfarm)

        # and restart the server
        server = process.server(dbname=mydb, mapiport = mapi_port, stdin=process.PIPE)

        # question is, is our data still there?
        conn3 = pymonetdb.connect(
            database=server.dbname, hostname='localhost',
            port=mapi_port,
            username="monetdb", password="monetdb",
            autocommit=False
        )
        cur3 = conn3.cursor()
        cur3.execute('select * from foo')
        foo = cur3.fetchall()

        # uncommitted1 and uncommitted2 should not be present
        assert foo == [('committed1',)]

        cleanup_afterward = True

    finally:
        if server:
            server.terminate()
            server.wait()
            if cleanup_afterward:
                shutil.rmtree(mydbdir)

main()

