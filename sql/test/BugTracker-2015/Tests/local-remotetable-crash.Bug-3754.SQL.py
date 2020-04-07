import os, sys, re
try:
    from MonetDBtesting import process
except ImportError:
    import process

with process.client('sql', stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as c:
    c.stdin.write('''\
CREATE TABLE t1 (i int);
CREATE REMOTE TABLE rt (LIKE t1) ON 'mapi:monetdb://localhost:{}/{}';
SELECT * FROM rt;
'''.format(os.environ['MAPIPORT'], os.environ['TSTDB']))
    out, err = c.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)
with process.client('sql', stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as c:
    c.stdin.write('''\
DROP TABLE rt;
DROP TABLE t1;
''')
    out, err = c.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)
