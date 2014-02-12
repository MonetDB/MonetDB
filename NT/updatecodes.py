#!/usr/bin/python

# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://www.monetdb.org/Legal/MonetDBLicense
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the MonetDB Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
# Copyright August 2008-2014 MonetDB B.V.
# All Rights Reserved.

import subprocess
import re

prcdre = re.compile('"ProductCode" = "8:{(.*)}"')
pacdre = re.compile('"PackageCode" = "8:{(.*)}"')

def update(f):
    p = subprocess.Popen(['uuidgen'], stdout = subprocess.PIPE)
    u, e = p.communicate()
    productcode = u.strip('\r\n').upper()
    p = subprocess.Popen(['uuidgen'], stdout = subprocess.PIPE)
    u, e = p.communicate()
    packagecode = u.strip('\r\n').upper()
    fp = open(f, 'rb')
    data = fp.read()
    fp.close()
    repl = '"ProductCode" = "8:{%s}"' % productcode
    data = prcdre.sub(repl, data)
    repl = '"PackageCode" = "8:{%s}"' % packagecode
    data = pacdre.sub(repl, data)
    fp = open(f, 'wb')
    fp.write(data)
    fp.close()

if __name__ == '__main__':
    import sys
    for f in sys.argv[1:]:
        update(f)
