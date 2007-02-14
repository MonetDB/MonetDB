import os

TSTTRGDIR = os.environ['TSTTRGDIR']
SQLCLIENT = os.environ['SQLCLIENT']
MAPIPORT = os.environ['MAPIPORT']

f = open(os.path.join(TSTTRGDIR, 'dumpoutput.sql'), 'r')

p = os.popen('%s -p %s' % (SQLCLIENT, MAPIPORT), 'w')

p.write(f.read())

p.close()
f.close()
