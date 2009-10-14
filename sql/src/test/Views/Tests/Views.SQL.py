import os, sys
import subprocess


def client(cmd, infile):
    clt = subprocess.Popen(cmd, shell=True, universal_newlines=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = clt.communicate(open(infile).read())
    sys.stdout.write(out)
    sys.stderr.write(err)


def main():
    clcmd = os.getenv('SQL_CLIENT')
    sys.stdout.write('Views Restrictions\n')
    client(clcmd, os.path.join(os.getenv('RELSRCDIR'), '..', 'views_restrictions.sql'))
    sys.stdout.write('step 1\n')
    sys.stdout.write('Cleanup\n')
    sys.stdout.write('step2\n')

main()
