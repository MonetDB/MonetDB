import os, sys

TSTTRGDIR = os.environ['TSTTRGDIR']
SQLDUMP = os.environ['SQLDUMP']
MAPIPORT = os.environ['MAPIPORT']

p = os.popen('%s -p %s' % (SQLDUMP, MAPIPORT), 'r')
dump = p.read()
p.close()

f = open(os.path.join(TSTTRGDIR, 'dumpoutput.sql'), 'w')
f.write(dump)
f.close()

sys.stdout.write(dump)
