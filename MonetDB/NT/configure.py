#!/usr/bin/env python

import re
import sys
import fileinput
import os

prefix=os.path.abspath(sys.argv[1])

subs = [
    ('@exec_prefix@',       "@prefix@"),
    ('@sysconfdir@',        "@prefix@@DIRSEP@etc"),
    ('@localstatedir@',     "@prefix@@DIRSEP@share"),
    ('@libdir@',            "@prefix@@DIRSEP@lib"),
    ('@bindir@',            "@prefix@@DIRSEP@bin"),
    ('@PACKAGE@',           "MonetDB"),
    ('@DIRSEP@',            "\\\\"),
    ('@prefix@',            prefix),
]


def substitute(line):
    for (p,v) in subs:
        line = re.sub(p,v,line);
    return line

for line in fileinput.input(sys.argv[2:]):
    sys.stdout.write(substitute(line))
