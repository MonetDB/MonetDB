import os
import string

TST = os.environ['TST']
TSTDB = os.environ['TSTDB']
TSTSRCDIR = os.environ['TSTSRCDIR']
XQ = os.path.join('XQ','Tests')
MIL = os.path.join('MIL','Tests')

CALL = 'pf "%s.xq" > xmark.mil' % os.path.join(TSTSRCDIR.replace(MIL,XQ),'xmark')

import sys, time
Mlog = "\n%s  %s\n\n" % (time.strftime('# %H:%M:%S >',time.localtime(time.time())), CALL)
sys.stdout.write(Mlog)
sys.stderr.write(Mlog)

os.system(CALL)
