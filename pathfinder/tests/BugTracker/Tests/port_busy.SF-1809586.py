import os, sys, socket
from MonetDBtesting import process

def prog(dbinit, input):
    srv = process.server('mil', dbinit = dbinit,
                        stdin = process.PIPE,
                        stdout = process.PIPE,
                        stderr = process.PIPE)
    out, err = srv.communicate(input)
    sys.stdout.write(out)
    sys.stderr.write(err)

def main():
    mserver = os.getenv('MSERVER')

    # test mapi and pathfinder modules with MAPIPORT busy
    port = int(os.getenv('MAPIPORT', '50000'))
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', port))
    prog('module(mapi);', 'print(1);\n')
    prog('module(pathfinder);', 'print(1);\n')
    s.close()

    # test mapi and pathfinder modules with XRPCPORT busy
    port = int(os.getenv('XRPCPORT', '50001'))
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', port))
    prog('module(mapi);', 'print(1);\n')
    prog('module(pathfinder);', 'print(1);\n')
    s.close()

main()
