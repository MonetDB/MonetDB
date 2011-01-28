#!/usr/bin/python

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
