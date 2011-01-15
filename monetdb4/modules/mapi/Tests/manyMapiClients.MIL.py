import sys
import os

try:
    from monetdb.mapi import Server
except SyntaxError:
    from monetdb.mapi25 import Server


def main(argv) :

    STDOUT = sys.stdout
    STDERR = sys.stderr

    n = 1234

    STDOUT.write("\n# %d Mapi-Client connections\n\n" % n)
    STDERR.write("\n# %d Mapi-Client connections\n\n" % n)

    i=0
    while i < n:
        i = i + 1
        STDOUT.write("%d:\n" % i)
        STDERR.write("%d:\n" % i)

        s = Server()
        s.connect(hostname = "localhost",
                  port = int(os.environ['MAPIPORT']),
                  username = 'monetdb',
                  password = 'monetdb',
                  database = 'demo',
                  language = 'mil')
        print( s.cmd("print(%d);\n" % i ) )
        s.disconnect()
    STDOUT.write("done: %d\n" % i)

### main(argv) #


if __name__ == "__main__":
    main(sys.argv)
