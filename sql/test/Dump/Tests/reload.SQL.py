import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

with process.client('sql', user = 'monetdb', passwd = 'monetdb',
                    stdin = open(os.path.join(os.environ['TSTTRGDIR'],
                                              'dumpoutput.sql'), 'r'),
                    stdout = process.PIPE, stderr = process.PIPE) as clt:
    out, err = clt.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)
