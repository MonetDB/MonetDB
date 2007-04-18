import os
import string

TST = os.environ['TST']
TSTDB = os.environ['TSTDB']
MSERVER = os.environ['MSERVER'].replace('--trace','')
TSTSRCDIR = os.environ['TSTSRCDIR']

CALL = 'pf --enable-standoff %s.xq | %s --set standoff=enabled --dbname=%s --dbinit="module(pathfinder);"' % (os.path.join(TSTSRCDIR,TST),MSERVER,TSTDB)

if os.name == "nt":
    os.system("call Mlog.bat '%s'" % CALL.replace('|','\\|'))
else:
    os.system("Mlog '%s'" % CALL.replace('|','\\|'))
os.system(CALL)
        