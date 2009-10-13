import os, sys
import subprocess


def client(cmd):
    clt = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    sys.stdout.write(clt.stdout.read())
    clt.stdout.close()
    sys.stderr.write(clt.stderr.read())
    clt.stderr.close()


def main():
    clcmd = str(os.getenv('SQL_CLIENT')) + "< %s" % ('%s/../views_restrictions.sql' % os.getenv('RELSRCDIR'))
    sys.stdout.write('Views Restrictions\n')
    client(clcmd)
    sys.stdout.write('step 1\n')
    sys.stdout.write('Cleanup\n')
    sys.stdout.write('step2\n')

main()
