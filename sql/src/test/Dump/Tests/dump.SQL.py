import os, sys
try:
    import subprocess
except ImportError:
    # use private copy for old Python versions
    import MonetDBtesting.subprocess26 as subprocess

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
