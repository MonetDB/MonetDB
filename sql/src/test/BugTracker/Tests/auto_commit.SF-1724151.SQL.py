import os, sys

cltcmd = "%s %s" % (os.getenv('SQL_CLIENT'), os.path.join(os.getenv('TSTSRCDIR'), sys.argv[1]+".txt"))

os.system(cltcmd);
