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
def client(input):
    c = process.client('sql', port=myport, dbname='db1', stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE)
    out, err = c.communicate(input)
    sys.stdout.write(out)
    sys.stderr.write(err)

script1 = '''\
create table t2 (a int);
'''

with tempfile.TemporaryDirectory() as farm_dir:
    os.mkdir(os.path.join(farm_dir, 'db1'))
    # Start the server without readonly one time to initialize catalog
    with process.server(mapiport=myport, dbname='db1',
                        dbfarm=os.path.join(farm_dir, 'db1'),
                        stdin=process.PIPE,
                        stdout=process.PIPE, stderr=process.PIPE) as s:
        out, err = s.communicate()
        sys.stdout.write(out)
        sys.stderr.write(err)
    with process.server(mapiport=myport, dbname='db1',
                        dbfarm=os.path.join(farm_dir, 'db1'),
                        args=["--set", "gdk_readonly=yes"],
                        stdin=process.PIPE,
                        stdout=process.PIPE, stderr=process.PIPE) as s:
        client(script1)
        out, err = s.communicate()
        sys.stdout.write(out)
        sys.stderr.write(err)
