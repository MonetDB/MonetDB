import os, sys
from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    d = tc.sqldump()
    d.assertMatchStableOut(fout='dump.stable.out')
    # this dump will be used in subsequent test
    with open(os.path.join(os.environ['TSTTRGDIR'], 'dumpoutput.sql'), 'w') as f:
        f.write(d.data)

#try:
#    from MonetDBtesting import process
#except ImportError:
#    import process
#
#with process.client('sqldump', stdout=process.PIPE, stderr=process.PIPE) as p:
#    dump, err = p.communicate()
#
#f = open(os.path.join(os.environ['TSTTRGDIR'], 'dumpoutput.sql'), 'w')
#f.write(dump)
#f.close()
#
#sys.stdout.write(dump)
#sys.stderr.write(err)
