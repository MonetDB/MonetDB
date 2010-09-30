import os, sys
from MonetDBtesting import process

clt = process.client('sql', user = 'monetdb', passwd = 'monetdb',
                     stdin = open(os.path.join(os.environ['TSTTRGDIR'],
                                               'dumpoutput.sql'), 'r'),
                     stdout = process.PIPE, stderr = process.PIPE)
out, err = clt.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
