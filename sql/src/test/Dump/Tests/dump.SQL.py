import os

TSTTRGDIR = os.environ['TSTTRGDIR']
SQLDUMP = os.environ['SQLDUMP']
MAPIPORT = os.environ['MAPIPORT']

f = open(os.path.join(TSTTRGDIR, 'dumpoutput.sql'), 'w')

p = os.popen('%s -p %s' % (SQLDUMP, MAPIPORT), 'r')

f.write(p.read())

p.close()
f.close()
