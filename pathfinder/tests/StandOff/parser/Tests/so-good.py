import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

c = process.pf(args = ['-b', '-s1',
                       '%s.xq' % os.path.join(os.environ['TSTSRCDIR'],
                                              os.environ['TST'])],
               stdout = process.PIPE, stderr = process.PIPE)
out, err = c.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
print c.returncode
