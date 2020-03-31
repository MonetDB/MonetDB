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

def server_start(args = []):
    sys.stderr.write('#mserver\n')
    sys.stderr.flush()
    srv = process.server(mapiport=myport, dbname='db1', dbfarm=os.path.join(farm_dir, 'db1'), stdin = process.PIPE,
                         stdout = process.PIPE, stderr = process.PIPE)
    return srv

def client(file):
    sys.stderr.write('#client\n')
    sys.stderr.flush()
    clt = process.client('sql', port = myport, dbname='db1', stdin = open(file),
                         stdout = process.PIPE, stderr = process.PIPE)
    return clt.communicate()

srv = None
try:
    srv = server_start(["--set", "sql_debug=64"])
    out, err = client(os.path.join(os.getenv('RELSRCDIR'),
                                   'set_sql_debug_64__breaking_the_DB.SF-1906287_create.sql'))
    sys.stdout.write(out)
    sys.stderr.write(err)
    out, err = srv.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)
    srv = server_start()
    out, err = client(os.path.join(os.getenv('RELSRCDIR'),
                                   'set_sql_debug_64__breaking_the_DB.SF-1906287_drop.sql'))
    sys.stdout.write(out)
    sys.stderr.write(err)
    out, err = srv.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)
finally:
    if srv is not None:
        srv.terminate()
    shutil.rmtree(farm_dir)
