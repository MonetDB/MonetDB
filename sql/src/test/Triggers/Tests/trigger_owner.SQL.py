import os, sys
try:
    import subprocess
except ImportError:
    # use private copy for old Python versions
    import MonetDB.subprocess26 as subprocess

def client(cmd):
    clt = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    sys.stdout.write(clt.stdout.read())
    clt.stdout.close()
    sys.stderr.write(clt.stderr.read())
    clt.stderr.close()



def main():
    clcmd = str(os.getenv('SQL_CLIENT')) + "< %s" % ('%s/../trigger_owner_create.sql' % os.getenv('RELSRCDIR'))
    clcmd1 = str(os.getenv('SQL_CLIENT')) + "-uuser_test -Ppass < %s" % ('%s/../trigger_owner.sql' % os.getenv('RELSRCDIR'))
    clcmd2 = str(os.getenv('SQL_CLIENT')) + "< %s" % ('%s/../trigger_owner_drop.sql' % os.getenv('RELSRCDIR'))
    sys.stdout.write('trigger owner\n')
    client(clcmd)
    client(clcmd1)
    client(clcmd2)
    sys.stdout.write('done\n')

main()
