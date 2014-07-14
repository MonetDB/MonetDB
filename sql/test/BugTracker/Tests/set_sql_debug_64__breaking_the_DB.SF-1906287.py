import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

def server_start(args = []):
    sys.stderr.write('#mserver\n')
    sys.stderr.flush()
    srv = process.server(args = args, stdin = process.PIPE,
                         stdout = process.PIPE, stderr = process.PIPE)
    return srv

def client(file):
    sys.stderr.write('#client\n')
    sys.stderr.flush()
    clt = process.client('sql', stdin = open(file),
                         stdout = process.PIPE, stderr = process.PIPE)
    return clt.communicate()

def main():
    srv = server_start(["--set", "sql_debug=64"])
    out, err = client(os.path.join(os.getenv('RELSRCDIR'),
                                   'set_sql_debug_64__breaking_the_DB.SF-1906287_create.sql'))
    sys.stdout.write(out)
    sys.stderr.write(err)
    out, err = srv.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)
    srv = server_start()
    out, err = client(os.path.join(os.getenv('RELSRCDIR'),
                                   'set_sql_debug_64__breaking_the_DB.SF-1906287_drop.sql'))
    sys.stdout.write(out)
    sys.stderr.write(err)
    out, err = srv.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)

main()
