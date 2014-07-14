import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

c = process.client('sql',
                   stdin = open(os.path.join(os.getenv('TSTSRCDIR'),
                                             os.path.pardir,
                                             'dumping_tables.SF-2776908.sql')),
                   stdout = process.PIPE, stderr = process.PIPE,
                   log = True)
out, err = c.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
