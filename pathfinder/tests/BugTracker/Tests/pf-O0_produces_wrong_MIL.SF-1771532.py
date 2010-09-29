import os
from MonetDBtesting import process

pf = process.pf(args = ['-O0', '%s.xq' % os.environ['TST']],
                stdout = process.PIPE, log = True)
srv = process.server(lang = 'xquery', stdin = pf.stdout,
                     log = True, notrace = True)
pf.communicate()
srv.communicate()
