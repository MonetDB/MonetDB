import os, sys
from MonetDBtesting import process

TSTSRCBASE = os.environ['TSTSRCBASE']
TSTDIR = os.environ['TSTDIR']
SRCDIR = os.path.join(TSTSRCBASE,TSTDIR,os.pardir)
DATADIR = os.path.join(SRCDIR,"SF-0.01") + os.sep.replace('\\', r'\\')

c = process.client('sql', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
for ln in open(os.path.join(SRCDIR,"load-sf-0.01-LOCKED.sql")):
    c.stdin.write(ln.replace('PWD/', DATADIR))
out, err = c.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
