import os, socket, sys, tempfile, pymonetdb

try:
    from MonetDBtesting import process
except ImportError:
    import process


# Find a free network port
def freeport():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('', 0))
    port = sock.getsockname()[1]
    sock.close()
    return port

with tempfile.TemporaryDirectory() as farm_dir:
    os.mkdir(os.path.join(farm_dir, 'db'))
    dport = freeport()

    with process.server(mapiport=dport, dbname='db',
                        dbfarm=os.path.join(farm_dir, 'db'),
                        stdin=process.PIPE, stdout=process.PIPE,
                        stderr=process.PIPE) as dproc:
        client1 = pymonetdb.connect(database='db', port=dport, autocommit=True)
        cur1 = client1.cursor()
        cur1.execute("""
        CREATE schema ft;
        CREATE FUNCTION ft.func() RETURNS TABLE (sch varchar(100)) RETURN TABLE (SELECT '1');
        """)
        cur1.execute("select * from ft.func() as ftf;")
        if cur1.fetchall() != [('1',)]:
            sys.stderr.write('Expected [(\'1\',)]')
        cur1.close()
        client1.close()

        dproc.communicate()

    with process.server(mapiport=dport, dbname='db',
                        dbfarm=os.path.join(farm_dir, 'db'),
                        stdin=process.PIPE, stdout=process.PIPE,
                        stderr=process.PIPE) as dproc:
        client1 = pymonetdb.connect(database='db', port=dport, autocommit=True)
        cur1 = client1.cursor()
        cur1.execute("select * from ft.func() as ftf;")
        if cur1.fetchall() != [('1',)]:
            sys.stderr.write('Expected [(\'1\',)]')
        cur1.execute("""
        drop function ft.func;
        drop schema ft;
        """)
        cur1.close()
        client1.close()

        dproc.communicate()
