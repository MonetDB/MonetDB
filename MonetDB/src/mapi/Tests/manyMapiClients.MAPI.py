import sys
import os

if os.environ.has_key('MONET_PREFIX'):
    sys.path.append(os.path.join(os.environ['MONET_PREFIX'],'share','MonetDB','python'))

import Mapi

STDOUT = sys.stdout
STDERR = sys.stderr

n = 195

STDOUT.write("\n# %d Mapi-Client connections\n\n" % n)
STDERR.write("\n# %d Mapi-Client connections\n\n" % n)

i=0;
while i < n:
    i = i + 1
    STDOUT.write("%d:\n" % i)
    STDERR.write("%d:\n" % i)

    s = Mapi.server( "localhost", int(os.environ['MAPIPORT']), os.environ['USER'])
    print( s.cmd( "print(%d);\n" % i ) )
    s.disconnect()
STDOUT.write("done: %d\n" % i)
