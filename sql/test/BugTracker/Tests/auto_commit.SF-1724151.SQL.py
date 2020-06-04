import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

with process.client('sql',
                    args = [os.path.join(os.getenv('TSTSRCDIR'),
                                         '%s.txt' % sys.argv[1])],
                    stdout = process.PIPE, stderr = process.PIPE) as c:
    out, err = c.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)
