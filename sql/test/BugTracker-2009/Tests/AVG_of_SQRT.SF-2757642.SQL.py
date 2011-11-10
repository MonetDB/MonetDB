import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

n = 100000000
clt = process.client('sql',
                     stdin = process.PIPE,
                     stdout = process.PIPE,
                     stderr = process.PIPE,
                     interactive = False,
                     echo = False)
clt.stdin.write('start transaction;\n')
clt.stdin.write('create table n8 (a numeric(14,2));\n')
clt.stdin.write('copy %d records into n8 from stdin;\n' % n)
s = '1.21\n' * 1000
for i in xrange(n / 1000):
    clt.stdin.write(s)
clt.stdin.write("select 'avg(sqrt(n8)) == 1.1', avg(sqrt(a)) from n8;\n")
clt.stdin.write('rollback;\n')
out, err = clt.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
