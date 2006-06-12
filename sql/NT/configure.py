#!/usr/bin/env python

# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the MonetDB Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-2006 CWI.
# All Rights Reserved.

import sys
import fileinput
import os

build=os.path.abspath(sys.argv[1]);
prefix=os.path.abspath(sys.argv[2]);

source=os.path.abspath(os.path.join(build,os.pardir))

subs = [
    ('@exec_prefix@',            prefix),
    ('@Xexec_prefix@',           prefix),
    ('@sysconfdir@',             r'${prefix}\etc'),
    ('@Xsysconfdir@',            r'${prefix}\etc'),
    ('@localstatedir@',          r'${prefix}\var'),
    ('@Xlocalstatedir@',         r'${prefix}\var'),
    ('@libdir@',                 r'${prefix}\lib'),
    ('@Xlibdir@',                r'${prefix}\lib'),
    ('@bindir@',                 r'${prefix}\bin'),
    ('@Xbindir@',                r'${prefix}\bin'),
    ('@mandir@',                 r'${prefix}\man'),
    ('@Xmandir@',                r'${prefix}\man'),
    ('@includedir@',             r'${prefix}\include'),
    ('@Xincludedir@',            r'${prefix}\include'),
    ('@datadir@',                r'${prefix}\share'),
    ('@Xdatadir@',               r'${prefix}\share'),
    ('@infodir@',                r'${prefix}\info'),
    ('@Xinfodir@',               r'${prefix}\info'),
    ('@libexecdir@',             r'${prefix}\libexec'),
    ('@Xlibexecdir@',            r'${prefix}\libexec'),
    ('@PACKAGE@',                r'sql'),
    ('@VERSION@',                r'2.12.0_rc1'),
    ('@DIRSEP@',                 '\\'),
    ('@prefix@',                 prefix),
    ('@Xprefix@',                prefix),
    ('@MONETDB_BUILD@',          build),
    ('@XMONETDB_BUILD@',         build),
    ('@MONETDB_SOURCE@',         source),
    ('@XMONETDB_SOURCE@',        source),
    ('@MONETDB_PREFIX@',         os.getenv('MONETDB_PREFIX')),
    ('@SQL_BACKEND@',            r'monet4'),
    ('@SQL_BUILD@',              os.getenv('SQL_BUILD')),
    ('@PYTHON@',                 'python'),
]

for key, val in subs[:]:
    subs.insert(0, ('@Q'+key[1:], val.replace('\\', r'\\')))


def substitute(line):
    for (p,v) in subs:
        line = line.replace(p, v);
    return line

for line in fileinput.input(sys.argv[3:]):
    sys.stdout.write(substitute(line))
