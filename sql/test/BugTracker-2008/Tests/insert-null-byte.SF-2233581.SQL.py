import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

c = process.client('sql',
                   stdin = open(os.path.join(os.getenv('TSTSRCDIR'),
                                             os.path.pardir,
                                             'insert-null-byte.sql')),
                   stdout = process.PIPE, stderr = process.PIPE)
out, err = c.communicate()
sys.stdout.write(out)
sys.stderr.write(err)

c = process.client('sql', stdin = process.PIPE,
                   stdout = process.PIPE, stderr = process.PIPE)
out, err = c.communicate('drop table strings2233581;')
sys.stdout.write(out)
sys.stderr.write(err)
