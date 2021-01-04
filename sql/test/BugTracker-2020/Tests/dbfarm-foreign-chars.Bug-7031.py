import sys, os, socket, tempfile, pymonetdb

try:
    from MonetDBtesting import process
except ImportError:
    import process

def freeport():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('', 0))
    port = sock.getsockname()[1]
    sock.close()
    return port

with tempfile.TemporaryDirectory() as farm_dir:
    mypath = os.path.join(farm_dir, '进起都家', 'myserver','mynode')
    os.makedirs(mypath)

    prt = freeport()
    with process.server(mapiport=prt, dbname='mynode', dbfarm=mypath,
                        stdin=process.PIPE, stdout=process.PIPE,
                        stderr=process.PIPE) as prc:
        conn = pymonetdb.connect(database='mynode', port=prt, autocommit=True)
        cur = conn.cursor()

        cur.execute('SELECT \'进起都家\';')
        if cur.fetchall() != [('进起都家',)]:
            sys.stderr.write("'进起都家' expected")

        cur.close()
        conn.close()
        prc.communicate()
