import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

TSTSRCDIR = os.environ['TSTSRCDIR']
DATADIR = (os.path.join(TSTSRCDIR,"SF-0.01") + os.sep).replace('\\', r'\\')

c = process.client('sql', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
for ln in open(os.path.join(TSTSRCDIR,"load-sf-0.01.sql")):
    c.stdin.write(ln.replace('PWD/', DATADIR))
out, err = c.communicate()
sys.stdout.write(out)
sys.stderr.write(err)

c = process.client('sql', stdin = open(os.path.join(TSTSRCDIR,"check1.sql")), stdout = process.PIPE, stderr = process.PIPE)
out, err = c.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
