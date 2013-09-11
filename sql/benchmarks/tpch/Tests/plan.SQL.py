import os, sys, re
try:
    from MonetDBtesting import process
except ImportError:
    import process

TSTSRCBASE = os.environ['TSTSRCBASE']
TSTDIR = os.environ['TSTDIR']
SRCDIR = os.path.join(TSTSRCBASE,TSTDIR)

c = process.client('sql', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
query = re.compile(r'^select\n')
stats = re.compile(r'^select \* from optimizer_stats\(\) stats;\n')
for ln in open(os.path.join(SRCDIR,"%s.sql" % os.environ['TST'][0:2])):
    if query.match(ln):
        c.stdin.write('plan select\n')
    elif not stats.match(ln):
        c.stdin.write(ln)
out, err = c.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
