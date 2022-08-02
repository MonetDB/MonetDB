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

myport = freeport()

with tempfile.TemporaryDirectory() as farm_dir:
    os.mkdir(os.path.join(farm_dir, 'db1'))
    with process.server(mapiport=myport, dbname='db1',
                        dbfarm=os.path.join(farm_dir, 'db1'),
                        stdin=process.PIPE,
                         stdout=process.PIPE, stderr=process.PIPE) as srv:
        with process.client('sql', port=myport, dbname='db1',
                            stdin=process.PIPE, stdout=process.PIPE,
                            stderr=process.PIPE) as c:
            out, err = c.communicate('call sys.shutdown(10);')
            sys.stdout.write(out)
            sys.stderr.write(err)
        out, err = srv.communicate()
        sys.stdout.write(out)
        sys.stderr.write(err)
