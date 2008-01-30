import os
import string

TST = os.environ['TST']
TSTDB = os.environ['TSTDB']
MSERVER = os.environ['MSERVER'].replace('--trace','')
PF = os.environ['PF']

CALL = '%s -M "%s.xq" | %s --dbname=%s "--dbinit=module(pathfinder); debugmask(and(debugmask(),xor(INT_MAX,8+2)));"' % (PF,TST,MSERVER,TSTDB)

import sys, time
Mlog = "\n%s  %s\n\n" % (time.strftime('# %H:%M:%S >',time.localtime(time.time())), CALL)
sys.stdout.write(Mlog)
sys.stderr.write(Mlog)

os.system(CALL)
