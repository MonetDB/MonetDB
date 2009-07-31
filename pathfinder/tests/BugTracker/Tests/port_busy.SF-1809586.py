import os, sys, socket
try:
    import subprocess
except ImportError:
    # user private copy for old Python versions
    import MonetDBtesting.subprocess26 as subprocess

def prog(cmd, input = None):
    clt = subprocess.Popen(cmd,
                           stdin = subprocess.PIPE,
                           stdout = subprocess.PIPE,
                           stderr = subprocess.PIPE,
                           universal_newlines = True,
                           shell = type(cmd) == type(''))
    out, err = clt.communicate(input)
    sys.stdout.write(out)
    sys.stderr.write(err)

def main():
    mserver = os.getenv('MSERVER')

    # test mapi and pathfinder modules with MAPIPORT busy
    port = os.getenv('MAPIPORT')
    if port:
        port = int(port)
    else:
        port = 50001
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', port))
    prog(mserver + ' "--dbinit=module(mapi);"', 'print(1);\n')
    prog(mserver + ' "--dbinit=module(pathfinder);"', 'print(1);\n')
    s.close()

    # test mapi and pathfinder modules with XRPCPORT busy
    port = os.getenv('XRPCPORT')
    if port:
        port = int(port)
    else:
        port = 50001
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', port))
    prog(mserver + ' "--dbinit=module(mapi);"', 'print(1);\n')
    prog(mserver + ' "--dbinit=module(pathfinder);"', 'print(1);\n')
    s.close()

main()
