#!/usr/bin/env python

# The contents of this file are subject to the MonetDB Public
# License Version 1.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of
# the License at 
# http://monetdb.cwi.nl/Legal/MonetDBPL-1.0.html
# 
# Software distributed under the License is distributed on an "AS
# IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
# implied. See the License for the specific language governing
# rights and limitations under the License.
# 
# The Original Code is the Monet Database System.
# 
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-2002 CWI.  
# All Rights Reserved.
# 
# Contributor(s):
# 		Martin Kersten <Martin.Kersten@cwi.nl>
# 		Peter Boncz <Peter.Boncz@cwi.nl>
# 		Niels Nes <Niels.Nes@cwi.nl>
# 		Stefan Manegold  <Stefan.Manegold@cwi.nl>

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
