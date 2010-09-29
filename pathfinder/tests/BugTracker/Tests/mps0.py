import os
from MonetDBtesting import process

pf = process.pf(args = ['-M', '%s.xq' % os.environ['TST']],
                stdout = process.PIPE, log = True)
srv = process.server(lang = 'xquery',
                     dbinit = 'module(pathfinder); debugmask(and(debugmask(),xor(INT_MAX,8+2)));',
                     stdin = pf.stdout, log = True, notrace = True)
pf.communicate()
srv.communicate()
