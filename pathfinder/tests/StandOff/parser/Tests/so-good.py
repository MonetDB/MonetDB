import os
import string

TST = os.environ['TST']
TSTSRCDIR = os.environ['TSTSRCDIR']

CALL = 'pf -b -s1 "%s.xq"' % (os.path.join(TSTSRCDIR,TST))

ret = os.system(CALL)
print ret
