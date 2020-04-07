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
                        stdout=process.PIPE, stderr=process.PIPE) as s:
        out, err = s.communicate()
        sys.stdout.write(out)
        sys.stderr.write(err)
    with process.server(mapiport=myport, dbname='db1',
                        dbfarm=os.path.join(farm_dir, 'db1'),
                        stdin=process.PIPE,
                        stdout=process.PIPE, stderr=process.PIPE) as s:
        with process.client(lang = 'sqldump',
                            port=myport,
                            dbname='db1',
                            stdin = process.PIPE,
                            stdout = process.PIPE,
                            stderr = process.PIPE,
                            server = s) as c:
            out, err = c.communicate()
            sys.stdout.write(out)
            sys.stderr.write(err)
        out, err = s.communicate()
        sys.stdout.write(out)
        sys.stderr.write(err)
