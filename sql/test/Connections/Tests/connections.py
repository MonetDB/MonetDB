import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

def remote_server_start(x,s):
    sys.stdout.write('\nserver %d%d\n' % (x,s))
    sys.stderr.write('\nserver %d%d\n' % (x,s))
    sys.stderr.flush()
    sys.stderr.write('#remote mserver\n')
    sys.stdout.flush()
    sys.stderr.flush()
    port = os.getenv('MAPIPORT', '50000')
    srv = process.server(mapiport = int(port) + 1,
                         dbname = '%s_test1' % os.getenv('TSTDB'),
                         stdin = process.PIPE,
                         stdout = process.PIPE, stderr = process.PIPE)
    return srv

def server_start(x,s):
    sys.stdout.write('\nserver %d%d\n' % (x,s))
    sys.stderr.write('\nserver %d%d\n' % (x,s))
    sys.stderr.flush()
    sys.stderr.write('#mserver\n')
    sys.stdout.flush()
    sys.stderr.flush()
    srv = process.server(stdin = process.PIPE,
                         stdout = process.PIPE, stderr = process.PIPE)
    return srv

def server_stop(srv):
    return srv.communicate()

def client_load_file(clt, port, file):
    f = open(file, 'r')
    for line in f:
        line = line.replace('port_num5', str(port+2))
        line = line.replace('port_num', str(port+1))
        clt.stdin.write(line)
    f.close()

def client(x,s, c, file):
    sys.stdout.write('\nserver %d%d, client %d\n' % (x,s,c))
    sys.stderr.write('\nserver %d%d, client %d\n' % (x,s,c))
    sys.stderr.flush()
    sys.stderr.write('#client%d\n' % x)
    sys.stdout.flush()
    sys.stderr.flush()
    clt = process.client('sql', stdin = process.PIPE,
                         stdout = process.PIPE, stderr = process.PIPE)
    port = int(os.getenv('MAPIPORT', '50000'))
    client_load_file(clt, port, file)
    return clt.communicate()

def clients(x):
    s = 0
    s += 1
    srv = server_start(x,s)

    s += 1
    remote_srv = remote_server_start(x,s)

    c = 0

    c += 1
    out, err = client(x, s, c,
                      os.path.join(os.getenv('RELSRCDIR'), os.pardir,
                                   'connections_syntax.sql'))
    sys.stdout.write(out)
    sys.stderr.write(err)
    sys.stdout.flush()
    sys.stderr.flush()

    c += 1
    out, err = client(x, s, c,
                      os.path.join(os.getenv('RELSRCDIR'), os.pardir,
                                   'connections_semantic.sql'))
    sys.stdout.write(out)
    sys.stderr.write(err)
    sys.stdout.flush()
    sys.stderr.flush()

    c += 1
    out, err = client(x, s, c,
                      os.path.join(os.getenv('RELSRCDIR'), os.pardir,
                                   'connections_default_values.sql'))
    sys.stdout.write(out)
    sys.stderr.write(err)
    sys.stdout.flush()
    sys.stderr.flush()

    out, err = server_stop(remote_srv)
    sys.stdout.write(out)
    sys.stderr.write(err)
    sys.stdout.flush()
    sys.stderr.flush()

    out, err = server_stop(srv)
    sys.stdout.write(out)
    sys.stderr.write(err)
    sys.stdout.flush()
    sys.stderr.flush()

def main():
    x = 0
    x += 1
    clients(x)

main()
