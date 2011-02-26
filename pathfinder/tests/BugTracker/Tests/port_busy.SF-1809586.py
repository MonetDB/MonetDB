import os, sys, socket
try:
    from MonetDBtesting import process
except ImportError:
    import process

def prog(dbinit, input):
    sys.stdout.write("%s\n" % dbinit)
    sys.stderr.write("%s\n" % dbinit)
    srv = process.server('mil', dbinit = dbinit,
                        stdin = process.PIPE,
                        stdout = process.PIPE,
                        stderr = process.PIPE)
    out, err = srv.communicate(input)
    sys.stdout.write(out)
    sys.stderr.write(err)

def main():
    # test mapi and pathfinder modules with MAPIPORT busy
    sys.stdout.write("MAPIPORT\n")
    sys.stderr.write("MAPIPORT\n")
    port = int(os.getenv('MAPIPORT', '50000'))
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', port))
    prog('module(mapi);', 'print(1);\n')
    prog('module(pathfinder);', 'print(1);\n')
    s.close()

    # test mapi and pathfinder modules with XRPCPORT busy
    sys.stdout.write("XRPCPORT\n")
    sys.stderr.write("XRPCPORT\n")
    port = int(os.getenv('XRPCPORT', '50001'))
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', port))
    prog('module(mapi);', 'print(1);\n')
    prog('module(pathfinder);', 'print(1);\n')
    s.close()

main()
