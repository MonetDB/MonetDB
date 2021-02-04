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

try:
    from MonetDBtesting import process
except ImportError:
    import process

import os
import shutil
import socket
import tarfile

import pymonetdb

dbfarm = os.getenv('GDK_DBFARM')
tstdb = os.getenv('TSTDB')

assert dbfarm
assert tstdb


def test_snapshot(z_extension, expected_initial_bytes, unpack=True):
    mydb = tstdb + '_snap'
    mydbdir = os.path.join(dbfarm, mydb)
    tarname = os.path.join(dbfarm, mydb + '.tar' + z_extension)

    # clean up remainder of earlier run
    if os.path.exists(mydbdir):
        shutil.rmtree(mydbdir)
    if os.path.exists(tarname):
        os.remove(tarname)

    try:
        # figure out a free port number
        s = socket.socket()
        s.bind(('0.0.0.0', 0))
        mapi_port = s.getsockname()[1]
        s.close()
        s = None

        # start the server
        with process.server(dbname=mydb, mapiport = mapi_port, stdin=process.PIPE) as server:
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
            try:
                cur2.execute("insert into foo values ('uncommitted2')")
            except:
                pass

            # then conn1 creates the snapshot
            cur1.execute("call sys.hot_snapshot(%(tarname)s)", dict(tarname=tarname))

            # we shut down the server and delete the dbdir
            cur1.close()
            conn1.close()
            cur2.close()
            conn2.close()
            server.terminate()
            server.wait()

        shutil.rmtree(mydbdir)

        # check if the file is actually compressed using the right algorithm
        with open(tarname, "rb") as f:
            initial_bytes = f.read(len(expected_initial_bytes))
            assert initial_bytes == expected_initial_bytes

        if not unpack:
            return

        # open with decompression
        if tarname.endswith('.bz2'):
            import bz2
            f = bz2.BZ2File(tarname, 'rb')
        elif tarname.endswith('.gz'):
            import gzip
            f = gzip.GzipFile(tarname, 'rb')
        elif tarname.endswith('.lz4'):
            import lz4.frame
            f = lz4.frame.LZ4FrameFile(tarname, 'rb')
        elif tarname.endswith('.xz'):
            import lzma
            f = lzma.LZMAFile(tarname, 'rb')
        else:
            f = open(tarname, 'rb')

        # and extract the tar file
        with tarfile.open(fileobj=f) as tar:
            tar.extractall(dbfarm)

        f.close()
        # and restart the server
        with process.server(dbname=mydb, mapiport = mapi_port, stdin=process.PIPE) as server:

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

            cur3.close()
            conn3.close()
            server.terminate()
            server.wait()

    finally:
        if os.path.exists(mydbdir):
            shutil.rmtree(mydbdir)
        if os.path.exists(tarname):
            os.remove(tarname)


if __name__ == "__main__":
    test_snapshot('', b'')

