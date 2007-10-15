import os
import fileinput
import string

TSTSRCBASE = os.environ['TSTSRCBASE']
TSTDIR = os.environ['TSTDIR']
SRCDIR = os.path.join(TSTSRCBASE,TSTDIR)
DATADIR = os.path.join(SRCDIR,"SF-0.01")
SQL_CLIENT = os.environ['SQL_CLIENT']

f = open("load.sql","w")
for i in fileinput.input(os.path.join(SRCDIR,"load-sf-0.01.sql")):
    x = string.split(i,"PWD/")
    if len(x) == 2:
        ln = x[0]+DATADIR+os.sep+x[1]
        ln = string.replace(ln, '\\', '\\\\')
        ln = string.replace(ln, '|\\\\n', '|\\n')
        f.write(ln)
    if len(x) == 1:
        f.write(x[0])
f.close()

CALL = SQL_CLIENT+" < load.sql"

if os.name == "nt":
    os.system("call Mlog.bat %s" % CALL)
else:
    os.system("Mlog '%s'" % CALL)
os.system(CALL)
