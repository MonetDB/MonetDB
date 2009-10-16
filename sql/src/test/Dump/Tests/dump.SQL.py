import os, sys
from MonetDBtesting import process

p = process.client('sqldump', stdout = process.PIPE)
dump, err = p.communicate()

f = open(os.path.join(os.environ['TSTTRGDIR'], 'dumpoutput.sql'), 'w')
f.write(dump)
f.close()

sys.stdout.write(dump)
