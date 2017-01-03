#!/usr/bin/python

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.

import subprocess
import re

prcdre = re.compile('"ProductCode" = "8:{(.*)}"')
pacdre = re.compile('"PackageCode" = "8:{(.*)}"')

def update(f):
    p = subprocess.Popen(['uuidgen'], stdout = subprocess.PIPE,
                         universal_newlines = True)
    u, e = p.communicate()
    productcode = u.strip('\n').upper()
    p = subprocess.Popen(['uuidgen'], stdout = subprocess.PIPE,
                         universal_newlines = True)
    u, e = p.communicate()
    packagecode = u.strip('\n').upper()
    fp = open(f)
    data = fp.read()
    fp.close()
    repl = '"ProductCode" = "8:{%s}"' % productcode
    data = prcdre.sub(repl, data)
    repl = '"PackageCode" = "8:{%s}"' % packagecode
    data = pacdre.sub(repl, data)
    fp = open(f, 'w')
    fp.write(data)
    fp.close()

if __name__ == '__main__':
    import sys
    for f in sys.argv[1:]:
        update(f)
