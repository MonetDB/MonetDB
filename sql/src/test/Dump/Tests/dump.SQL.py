import os, sys
import subprocess

TSTTRGDIR = os.environ['TSTTRGDIR']
SQLDUMP = os.environ['SQLDUMP']
MAPIPORT = os.environ['MAPIPORT']

p = subprocess.Popen('%s -p %s' % (SQLDUMP, MAPIPORT),
                     shell = True,
                     universal_newlines = True,
                     stdout = subprocess.PIPE)
dump, err = p.communicate()

f = open(os.path.join(TSTTRGDIR, 'dumpoutput.sql'), 'w')
f.write(dump)
f.close()

sys.stdout.write(dump)
