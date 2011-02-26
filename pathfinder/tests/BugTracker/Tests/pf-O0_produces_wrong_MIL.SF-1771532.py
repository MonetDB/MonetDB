import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

pf = process.pf(args = ['-O0', '%s.xq' % os.environ['TST']],
                stdout = process.PIPE, stderr = process.PIPE, log = True)
srv = process.server(lang = 'xquery', stdin = pf.stdout,
                     stdout = process.PIPE, stderr = process.PIPE,
                     log = True, notrace = True)
pf.stdout = None                        # given away
out, err = pf.communicate()
sys.stderr.write(err)
out, err = srv.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
