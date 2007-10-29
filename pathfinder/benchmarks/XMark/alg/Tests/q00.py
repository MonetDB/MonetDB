import os
import string

TST = os.environ['TST']
TSTDB = os.environ['TSTDB']
MSERVER = os.environ['MSERVER'].replace('--trace','')
TSTSRCBASE = os.environ['TSTSRCBASE']

CALL = 'pf -A "%s.xq" | %s --dbname=%s "--dbinit=module(pathfinder);"' % (os.path.join(TSTSRCBASE,'benchmarks','XMark','Tests',TST),MSERVER,TSTDB)

import sys, time
Mlog = "\n%s  %s\n\n" % (time.strftime('# %H:%M:%S >',time.localtime(time.time())), CALL)
sys.stdout.write(Mlog)
sys.stderr.write(Mlog)

os.system(CALL)
