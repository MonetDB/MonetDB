import os
import string

TST = os.environ['TST']
TSTDB = os.environ['TSTDB']
MSERVER = os.environ['MSERVER'].replace('--trace','')
TSTSRCBASE = os.environ['TSTSRCBASE']

CALL = 'pf -A "%s.xq" | %s --dbname=%s "--dbinit=module(pathfinder);"' % (os.path.join(TSTSRCBASE,'benchmarks','XMark','Tests',TST),MSERVER,TSTDB)

if os.name == "nt":
    os.system('Mlog "%s"' % CALL)
else:
    os.system("Mlog '%s'" % CALL.replace('|','\\|'))
os.system(CALL)
