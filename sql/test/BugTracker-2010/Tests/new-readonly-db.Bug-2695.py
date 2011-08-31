try:
    from MonetDBtesting import process
except ImportError:
    import process
import os

s = process.server(args = ['--readonly'],
                   dbname = '%s-2695' % os.getenv('TSTDB'))
s.wait()
