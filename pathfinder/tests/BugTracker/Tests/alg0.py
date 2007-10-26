import os
import string

TST = os.environ['TST']
TSTDB = os.environ['TSTDB']
MSERVER = os.environ['MSERVER'].replace('--trace','')

CALL = 'pf -A "%s.xq" | %s --dbname=%s "--dbinit=module(pathfinder); debugmask(and(debugmask(),xor(INT_MAX,8+2)));"' % (TST,MSERVER,TSTDB)

os.system('Mlog "%s"' % CALL)
os.system(CALL)
