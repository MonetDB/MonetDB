import os, sys
import subprocess

def main():
    TSTTRGDIR = os.environ['TSTTRGDIR']
    SQLCLIENT = os.environ['SQLCLIENT']
    MAPIPORT = os.environ['MAPIPORT']

    cmd = str('%s -p %s' % (SQLCLIENT, MAPIPORT))
    f = open(os.path.join(TSTTRGDIR, 'dumpoutput.sql'), 'r')
    clt = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                           stdin=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = clt.communicate(f.read())
    f.close()
    sys.stdout.write(out)
    sys.stderr.write(err)


main()
