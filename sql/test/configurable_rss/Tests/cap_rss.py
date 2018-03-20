import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

def server_start(args):
    sys.stderr.write('#mserver: "%s"\n' % ' '.join(args))
    sys.stderr.flush()
    srv = process.server(args = args, stdin = process.PIPE,
                         stdout = process.PIPE, stderr = process.PIPE)
    return srv

def client(lang, file):
    sys.stderr.write('#client: "%s"\n' % file)
    sys.stderr.flush()
    clt = process.client(lang.lower(), stdin = open(file),
                         stdout = process.PIPE, stderr = process.PIPE)
    return clt.communicate()

def main():
    srv = server_start(["--set", "gdk_mem_maxsize=20000000"])

    out, err = client('SQL',
                      os.path.join(os.getenv('RELSRCDIR'),
                                   'push_and_check_rss.sql'))
    sys.stdout.write(out)
    sys.stderr.write(err)
    out, err = srv.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)

main()
