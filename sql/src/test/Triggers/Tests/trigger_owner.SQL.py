import os, sys
import subprocess

def client(cmd, infile):
    clt = subprocess.Popen(cmd, shell=True, universal_newlines=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = clt.communicate(open(infile).read())
    sys.stdout.write(out)
    sys.stderr.write(err)



def main():
    clcmd = os.getenv('SQL_CLIENT')
    clcmd1 = os.getenv('SQL_CLIENT') + " -uuser_test -Ppass"
    clcmd2 = os.getenv('SQL_CLIENT')
    relsrcdir = os.getenv('RELSRCDIR')
    sys.stdout.write('trigger owner\n')
    client(clcmd, os.path.join(relsrcdir, '..', 'trigger_owner_create.sql'))
    client(clcmd1, os.path.join(relsrcdir, '..', 'trigger_owner.sql'))
    client(clcmd2, os.path.join(relsrcdir, '..', 'trigger_owner_drop.sql'))
    sys.stdout.write('done\n')

main()
