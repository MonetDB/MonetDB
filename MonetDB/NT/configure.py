#!/usr/bin/env python

import re
import sys
import fileinput

subs = [
    ('@prefix@',            'prefix'),
    ('@exec_prefix@',       'prefix'),
    ('@sysconfdir@',        'prefix/etc'),
    ('@localstatedir@',     'prefix/share'),
    ('@libdir@',            'prefix/lib'),
    ('@bindir@',            'prefix/bin'),
    ('@PACKAGE@',           'MonetDB')
]

def substitute(line):
    for (p,v) in subs:
        line = re.sub(p,v,line);
    return line

for line in fileinput.input():
    sys.stdout.write(substitute(line))
