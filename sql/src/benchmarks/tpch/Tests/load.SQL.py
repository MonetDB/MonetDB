import os
import fileinput
import string

TSTSRCBASE = os.environ['TSTSRCBASE']
TSTDIR = os.environ['TSTDIR']
SRCDIR = os.path.join(TSTSRCBASE,TSTDIR)
DATADIR = os.path.join(SRCDIR,"SF-0.01")
SQL_CLIENT = os.environ['SQL_CLIENT']

f = open("load.sql","w")
for i in fileinput.input(os.path.join(SRCDIR,"load.sql")):
    x = string.split(i,"PWD/")
    if len(x) == 2:
        f.write(string.replace(x[0]+DATADIR+os.sep+x[1], '\\', '\\\\'))
    if len(x) == 1:
        f.write(x[0])
f.close()

CALL = SQL_CLIENT+" < load.sql"

os.system("Mlog '%s'" % CALL)
os.system(CALL)
