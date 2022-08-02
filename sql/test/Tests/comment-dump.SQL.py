import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

with process.client('sql',
                    stdin=open(os.path.join(os.getenv('TSTSRCDIR'),
                                              'comment-dump.sql')),
                    stdout=process.PIPE, stderr=process.PIPE,
                    log=True) as c:
    out, err = c.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)

with process.client('sqldump',
                    stdout=process.PIPE, stderr=process.PIPE,
                    log=True) as d:
    out, err = d.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)

with process.client('sqldump', args=['-f'],
                    stdout=process.PIPE, stderr=process.PIPE,
                    log=True) as d2:
    out, err = d2.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)

with process.client('sql',
                    stdin=open(os.path.join(os.getenv('TSTSRCDIR'),
                                              'comment-dump-cleanup.sql')),
                    stdout=process.PIPE, stderr=process.PIPE,
                    log=True) as p:
    out, err = p.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)
