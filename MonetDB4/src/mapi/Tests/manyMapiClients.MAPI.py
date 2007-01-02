import sys
import os

if os.environ.has_key('CLIENTS_PREFIX'):
    sys.path.append(os.path.join(os.environ['CLIENTS_PREFIX'],'share','MonetDB','python'))

import Mapi


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

        s = Mapi.server( "localhost", int(os.environ['MAPIPORT']), 'Mtest.py')
        print( s.cmd( "print(%d);\n" % i ) )
        s.disconnect()
    STDOUT.write("done: %d\n" % i)

### main(argv) #


if __name__ == "__main__":
    if '--trace' in sys.argv:
        sys.argv.remove('--trace')
        import trace
        t = trace.Trace(trace=1, count=0)
        t.runfunc(main, sys.argv)
    else:
        main(sys.argv)
