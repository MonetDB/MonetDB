#!/usr/bin/env python

import re
import sys
import fileinput
import os

prefix=os.path.abspath(sys.argv[1])
build=prefix
source=os.path.abspath(os.path.join(build,os.pardir))

subs = [
    ('@exec_prefix@',       "@prefix@"),
    ('@sysconfdir@',        "@prefix@@DIRSEP@etc"),
    ('@localstatedir@',     "@prefix@@DIRSEP@var"),
    ('@libdir@',            "@prefix@@DIRSEP@lib"),
    ('@bindir@',            "@prefix@@DIRSEP@bin"),
    ('@mandir@',            "@prefix@@DIRSEP@man"),
    ('@includedir@',        "@prefix@@DIRSEP@include"),
    ('@datadir@',           "@prefix@@DIRSEP@share"),
    ('@infodir@',           "@prefix@@DIRSEP@info"),
    ('@libexecdir@',        "@prefix@@DIRSEP@libexec"),
    ('@PACKAGE@',           "MonetDB"),
    ('@VERSION@',           "4.3.5"),
    ('@DIRSEP@',            "\\\\"),
    ('@prefix@',            prefix),
    ('@MONET_BUILD@',       build),
    ('@MONET_SOURCE@',      source),
]


def substitute(line):
    for (p,v) in subs:
        line = re.sub(p,v,line);
    return line

for line in fileinput.input(sys.argv[2:]):
    sys.stdout.write(substitute(line))
