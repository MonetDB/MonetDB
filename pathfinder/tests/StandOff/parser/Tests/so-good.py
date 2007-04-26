import os
import string

TST = os.environ['TST']
TSTSRCDIR = os.environ['TSTSRCDIR']

CALL = "pf -b -s1 %s.xq" % (os.path.join(TSTSRCDIR,TST))

if os.name == "nt":
    os.system("%s & echo %ERRORLEVEL%" % CALL)
else:
    os.system("%s ; echo $?" % CALL)
