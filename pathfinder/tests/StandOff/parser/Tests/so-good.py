import os
import string

TST = os.environ['TST']
TSTSRCDIR = os.environ['TSTSRCDIR']
PF = os.environ['PF']

CALL = '%s -b -s1 "%s.xq"' % (PF,os.path.join(TSTSRCDIR,TST))

ret = os.system(CALL)
print ret
