from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    with open('dumpoutput2.sql') as f:
        tc.execute(query=None, stdin=f, client='mclient')\
                .assertSucceeded()\
                .assertMatchStableOut('reload-again.stable.out')


#import os, sys
#try:
#    from MonetDBtesting import process
#except ImportError:
#    import process
#
#with process.client('sql', user = 'monetdb', passwd = 'monetdb',
#                    stdin = open(os.path.join(os.environ['TSTTRGDIR'],
#                                              'dumpoutput2.sql'), 'r'),
#                    stdout = process.PIPE, stderr = process.PIPE) as clt:
#    out, err = clt.communicate()
#    sys.stdout.write(out)
#    sys.stderr.write(err)
