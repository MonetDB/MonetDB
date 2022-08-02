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
    def __init__(self, args):
        sys.stderr.write('#mserver: "%s"\n' % ' '.join(args))
        sys.stderr.flush()
        super().__init__(mapiport=myport, dbname='db1',
                         dbfarm=os.path.join(farm_dir, 'db1'),
                         stdin=process.PIPE, stdout=process.PIPE,
                         stderr=process.PIPE)

def client(lang, file):
    sys.stderr.write('#client: "%s"\n' % file)
    sys.stderr.flush()
    clt = process.client(lang.lower(), port=myport, dbname='db1', stdin=open(file),
                         stdout=process.PIPE, stderr=process.PIPE)
    return clt.communicate()

with tempfile.TemporaryDirectory() as farm_dir:
    os.mkdir(os.path.join(farm_dir, 'db1'))
    myport = freeport()

    with server_start(["--set", "sql_debug=64"]) as srv:
        out, err = client('SQL',
                          os.path.join(os.getenv('RELSRCDIR'),
                                       'mdb_starts_with_sql_debug_64.SF-1999354.sql'))
        sys.stdout.write(out)
        sys.stderr.write(err)
        out, err = srv.communicate()
        sys.stdout.write(out)
        sys.stderr.write(err)
