import sys
import os

try:
    # assume MonetDB & MonetDB-Clients share the same prefix,
    # and their hence shared destination PYTHON_LIBDIR
    # is in Python's default search path,
    # or `monetdb-config --pythonlibdir` is in PYTHONPATH
    from MonetDB.Mapi import server
except ImportError:
    # assume MonetDB-Clients uses a different prefix than MonetDB,
    # and MonetDB-Clients' destination PYTHON_LIBDIR/MonetDB
    # is in Python's default search path,
    # or `monetdb-clients-config --pythonlibdir`/MonetDB is in PYTHONPATH
    from Mapi import server


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
