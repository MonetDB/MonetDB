import os
import string

TST = os.environ['TST']
TSTDB = os.environ['TSTDB']
MSERVER = os.environ['MSERVER'].replace('--trace','')

CALL = 'pf -O0 "%s.xq" | %s --dbname=%s "--dbinit=module(pathfinder);"' % (TST,MSERVER,TSTDB)

import sys, time
Mlog = "\n%s  %s\n\n" % (time.strftime('# %H:%M:%S >',time.localtime(time.time())), CALL)
sys.stdout.write(Mlog)
sys.stderr.write(Mlog)

os.system(CALL)
