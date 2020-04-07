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
    def __init__(self):
        sys.stderr.write('#mserver\n')
        sys.stderr.flush()
        super().__init__(mapiport=myport, dbname='db1',
                         dbfarm=os.path.join(farm_dir, 'db1'),
                         stdin=process.PIPE, stdout=process.PIPE,
                         stderr=process.PIPE)

def client(lang, file, user='monetdb', passwd='monetdb'):
    sys.stderr.write('#client\n')
    sys.stderr.flush()
    clt = process.client(lang, port=myport, dbname='db1',
                         user=user, passwd=passwd,
                         stdin=open(file),
                         stdout=process.PIPE, stderr=process.PIPE)
    return clt.communicate()

with tempfile.TemporaryDirectory() as farm_dir:
    os.mkdir(os.path.join(farm_dir, 'db1'))
    myport = freeport()

    with server_start() as srv:
        out, err = client('sql',
                          os.path.join(os.getenv('RELSRCDIR'),
                                       'set_a_new_user_password.SF-1844050_create_user.sql'))
        sys.stdout.write(out)
        sys.stderr.write(err)
        out, err = srv.communicate()
        sys.stdout.write(out)
        sys.stderr.write(err)

    with server_start() as srv:
        out, err = client('sql',
                          os.path.join(os.getenv('RELSRCDIR'),
                                       'set_a_new_user_password.SF-1844050_select.sql'),
                          "voc2", "new")
        sys.stdout.write(out)
        sys.stderr.write(err)
        out, err = srv.communicate()
        sys.stdout.write(out)
        sys.stderr.write(err)

    with server_start() as srv:
        out, err = client('sql',
                          os.path.join(os.getenv('RELSRCDIR'),
                                       'set_a_new_user_password.SF-1844050_drop_user.sql'))
        sys.stdout.write(out)
        sys.stderr.write(err)
        out, err = srv.communicate()
        sys.stdout.write(out)
        sys.stderr.write(err)
