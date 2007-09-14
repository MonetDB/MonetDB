import sys
import os

# assume MonetDB-Clients destination PYTHON_LIBDIR 
# is in Python's default search path,
# or `monetdb-clients-config --pythonlibdir` is in PYTHONPATH
from MonetDB.Mapi import server


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

        s = server( "localhost", int(os.environ['MAPIPORT']), 'Mtest.py')
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
