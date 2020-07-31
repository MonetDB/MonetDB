import os, socket, sys, tempfile, shutil
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

farm_dir = tempfile.mkdtemp()
os.mkdir(os.path.join(farm_dir, 'db1'))
myport = freeport()

def server_start(args):
    sys.stderr.write('#mserver: "%s"\n' % ' '.join(args))
    sys.stderr.flush()
    srv = process.server(mapiport=myport, dbname='db1', dbfarm=os.path.join(farm_dir, 'db1'), stdin = process.PIPE,
                         stdout = process.PIPE, stderr = process.PIPE)
    return srv

def client(lang, file):
    sys.stderr.write('#client: "%s"\n' % file)
    sys.stderr.flush()
    clt = process.client(lang.lower(), port=myport, dbname='db1', stdin = open(file),
                         stdout = process.PIPE, stderr = process.PIPE)
    return clt.communicate()

srv = None
try:
    srv = server_start(["--set", "sql_debug=64"])
    out, err = client('SQL',
                      os.path.join(os.getenv('RELSRCDIR'),
                                   'mdb_starts_with_sql_debug_64.SF-1999354.sql'))
    sys.stdout.write(out)
    sys.stderr.write(err)
    out, err = srv.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)
finally:
    if srv is not None:
        srv.terminate()
    shutil.rmtree(farm_dir)
