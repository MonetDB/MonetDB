import os, sys
try:
    import subprocess
except ImportError:
    # use private copy for old Python versions
    import MonetDBtesting.subprocess26 as subprocess

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
