import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

c = process.client('sql',
                   stdin = open(os.path.join(os.getenv('TSTSRCDIR'),
                                             'comment-dump.sql')),
                   stdout = process.PIPE, stderr = process.PIPE,
                   log = True)
out, err = c.communicate()
sys.stdout.write(out)
sys.stderr.write(err)

d = process.client('sqldump',
                   stdout = process.PIPE, stderr = process.PIPE,
                   log = True)
out, err = d.communicate()
sys.stdout.write(out)
sys.stderr.write(err)

d2 = process.client('sqldump', args=['-f'],
                   stdout = process.PIPE, stderr = process.PIPE,
                   log = True)
out, err = d2.communicate()
sys.stdout.write(out)
sys.stderr.write(err)

p = process.client('sql',
                   stdin = open(os.path.join(os.getenv('TSTSRCDIR'),
                                             'comment-dump-cleanup.sql')),
                   stdout = process.PIPE, stderr = process.PIPE,
                   log = True)
out, err = p.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
