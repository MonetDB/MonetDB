from __future__ import print_function

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

s = None
try:
    s = process.server(mapiport=myport, dbname='db1', dbfarm=os.path.join(farm_dir, 'db1'), stdin = process.PIPE,
                       stdout = process.PIPE, stderr = process.PIPE)
    out, err = s.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)
    s = process.server(mapiport=myport, dbname='db1', dbfarm=os.path.join(farm_dir, 'db1'), stdin = process.PIPE,
                       stdout = process.PIPE, stderr = process.PIPE)
    c = process.client(lang = 'sqldump',
                       port=myport,
                       dbname='db1',
                       stdin = process.PIPE,
                       stdout = process.PIPE,
                       stderr = process.PIPE,
                       server = s)
    out, err = c.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)
    out, err = s.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)
finally:
    if s is not None:
        s.terminate()
    shutil.rmtree(farm_dir)
