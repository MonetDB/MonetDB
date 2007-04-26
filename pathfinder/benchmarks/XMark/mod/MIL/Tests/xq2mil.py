import os
import string

TST = os.environ['TST']
TSTDB = os.environ['TSTDB']
TSTSRCDIR = os.environ['TSTSRCDIR']
XQ = os.path.join('XQ','Tests')
MIL = os.path.join('MIL','Tests')

CALL = "pf %s.xq > xmark.mil" % os.path.join(TSTSRCDIR.replace(MIL,XQ),'xmark')

if os.name == "nt":
    os.system("call Mlog.bat '%s'" % CALL.replace('>','\\>'))
else:
    os.system("Mlog '%s'" % CALL.replace('>','\\>'))
os.system(CALL)
