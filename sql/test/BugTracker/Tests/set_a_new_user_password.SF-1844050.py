import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

def server_start():
    sys.stderr.write('#mserver\n')
    sys.stderr.flush()
    srv = process.server(stdin = process.PIPE,
                         stdout = process.PIPE, stderr = process.PIPE)
    return srv

def client(lang, file, user = 'monetdb', passwd = 'monetdb'):
    sys.stderr.write('#client\n')
    sys.stderr.flush()
    clt = process.client(lang, user = user, passwd = passwd,
                         stdin = open(file),
                         stdout = process.PIPE, stderr = process.PIPE)
    return clt.communicate()

def main():
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

main()
