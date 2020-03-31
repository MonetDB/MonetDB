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

def server_start():
    sys.stderr.write('#mserver\n')
    sys.stderr.flush()
    srv = process.server(mapiport=myport, dbname='db1', dbfarm=os.path.join(farm_dir, 'db1'), stdin = process.PIPE,
                         stdout = process.PIPE, stderr = process.PIPE)
    return srv

def client(lang, file, user = 'monetdb', passwd = 'monetdb'):
    sys.stderr.write('#client\n')
    sys.stderr.flush()
    clt = process.client(lang, port = myport, dbname='db1', user = user, passwd = passwd,
                         stdin = open(file),
                         stdout = process.PIPE, stderr = process.PIPE)
    return clt.communicate()

srv = None
try:
    srv = server_start()
    out, err = client('sql',
                      os.path.join(os.getenv('RELSRCDIR'),
                                   'set_a_new_user_password.SF-1844050_create_user.sql'))
    sys.stdout.write(out)
    sys.stderr.write(err)
    out, err = srv.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)

    srv = server_start()
    out, err = client('sql',
                      os.path.join(os.getenv('RELSRCDIR'),
                                   'set_a_new_user_password.SF-1844050_select.sql'),
                      "voc2", "new")
    sys.stdout.write(out)
    sys.stderr.write(err)
    out, err = srv.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)

    srv = server_start()
    out, err = client('sql',
                      os.path.join(os.getenv('RELSRCDIR'),
                                   'set_a_new_user_password.SF-1844050_drop_user.sql'))
    sys.stdout.write(out)
    sys.stderr.write(err)
    out, err = srv.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)
finally:
    if srv is not None:
        srv.terminate()
    shutil.rmtree(farm_dir)
