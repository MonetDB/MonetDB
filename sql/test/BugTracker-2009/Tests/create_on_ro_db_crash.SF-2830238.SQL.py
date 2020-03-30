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

def client(input):
    c = process.client('sql', port = myport, dbname='db1', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
    out, err = c.communicate(input)
    sys.stdout.write(out)
    sys.stderr.write(err)

script1 = '''\
create table t2 (a int);
'''

s = None
try:
    s = process.server(mapiport=myport, dbname='db1', dbfarm=os.path.join(farm_dir, 'db1'), stdin = process.PIPE,
                       stdout = process.PIPE, stderr = process.PIPE) # Start the server without readonly one time to initialize catalog
    out, err = s.communicate()
    s = None
    sys.stdout.write(out)
    sys.stderr.write(err)
    s = process.server(args = ["--set", "gdk_readonly=yes"], mapiport=myport, dbname='db1', dbfarm=os.path.join(farm_dir, 'db1'), stdin = process.PIPE,
                       stdout = process.PIPE, stderr = process.PIPE)
    client(script1)
    out, err = s.communicate()
    s = None
    sys.stdout.write(out)
    sys.stderr.write(err)
finally:
    if s is not None:
        s.terminate()
    shutil.rmtree(farm_dir)
