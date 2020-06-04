import os, socket, sys, tempfile
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

class server_start(process.server):
    def __init__(self, args=[]):
        sys.stderr.write('#mserver\n')
        sys.stderr.flush()
        super().__init__(mapiport=myport, dbname='db1',
                         dbfarm=os.path.join(farm_dir, 'db1'),
                         stdin=process.PIPE, stdout=process.PIPE,
                         stderr=process.PIPE)

def client(file):
    sys.stderr.write('#client\n')
    sys.stderr.flush()
    with process.client('sql', port=myport, dbname='db1', stdin=open(file),
                        stdout=process.PIPE, stderr=process.PIPE) as clt:
        return clt.communicate()

with tempfile.TemporaryDirectory() as farm_dir:
    os.mkdir(os.path.join(farm_dir, 'db1'))
    myport = freeport()

    with server_start(["--set", "sql_debug=64"]) as srv:
        out, err = client(os.path.join(os.getenv('RELSRCDIR'),
                                       'set_sql_debug_64__breaking_the_DB.SF-1906287_create.sql'))
        sys.stdout.write(out)
        sys.stderr.write(err)
        out, err = srv.communicate()
        sys.stdout.write(out)
        sys.stderr.write(err)
    with server_start() as srv:
        out, err = client(os.path.join(os.getenv('RELSRCDIR'),
                                       'set_sql_debug_64__breaking_the_DB.SF-1906287_drop.sql'))
        sys.stdout.write(out)
        sys.stderr.write(err)
        out, err = srv.communicate()
        sys.stdout.write(out)
        sys.stderr.write(err)
