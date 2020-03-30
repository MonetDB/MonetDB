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

srv = None
try:
    srv = process.server(mapiport=myport, dbname='db1', dbfarm=os.path.join(farm_dir, 'db1'), stdin = process.PIPE,
                         stdout = process.PIPE, stderr = process.PIPE)

    c = process.client('sql', port=myport, dbname='db1', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
    out, err = c.communicate('call sys.shutdown(10);')
    sys.stdout.write(out)
    sys.stderr.write(err)

    out, err = srv.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)
finally:
    if srv is not None:
        srv.terminate()
    shutil.rmtree(farm_dir)
