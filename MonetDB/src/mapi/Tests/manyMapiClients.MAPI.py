# The contents of this file are subject to the MonetDB Public
# License Version 1.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of
# the License at
# http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
# 
# Software distributed under the License is distributed on an "AS
# IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
# implied. See the License for the specific language governing
# rights and limitations under the License.
# 
# The Original Code is the Monet Database System.
# 
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-2003 CWI.
# All Rights Reserved.
# 
# Contributor(s):
# 		Martin Kersten <Martin.Kersten@cwi.nl>
# 		Peter Boncz <Peter.Boncz@cwi.nl>
# 		Niels Nes <Niels.Nes@cwi.nl>
# 		Stefan Manegold  <Stefan.Manegold@cwi.nl>

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

i=0
while i < n:
    i = i + 1
    STDOUT.write("%d:\n" % i)
    STDERR.write("%d:\n" % i)

    s = Mapi.server( "localhost", int(os.environ['MAPIPORT']), 'Mtest.py')
    print( s.cmd( "print(%d);\n" % i ) )
    s.disconnect()
STDOUT.write("done: %d\n" % i)
