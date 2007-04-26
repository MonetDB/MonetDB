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
# Portions created by CWI are Copyright (C) 1997-2007 CWI.
# All Rights Reserved.

import sys
import fileinput
import os

subs = [("@bindir@", r'${exec_prefix}\bin'),
        ("@sbindir@", r'${exec_prefix}\sbin'),
        ("@libexecdir@", r'${exec_prefix}\libexec'),
        ("@datadir@", r'${prefix}\share'),
        ("@sysconfdir@", r'${prefix}\etc'),
        ("@sharedstatedir@", r'${prefix}\com'),
        ("@localstatedir@", r'${prefix}\var'),
        ("@libdir@", r'${exec_prefix}\lib'),
        ("@infodir@", r'${prefix}\info'),
        ("@mandir@", r'${prefix}\man'),
        ("@includedir@", r'${prefix}\include'),
        ("@oldincludedir@", r'\usr\include'),
        ("@pkgdatadir@", r'${prefix}\share\@PACKAGE@'),
        ("@pkglibdir@", r'${exec_prefix}\lib\@PACKAGE@'),
        ("@pkgincludedir@", r'${prefix}\include\@PACKAGE@'),
        ("@DIRSEP@", '\\'),
        ("@CROSS_COMPILING_FALSE@", ''),
        ("@LINK_STATIC_FALSE@", ''),
        ("@NATIVE_WIN32_FALSE@", '#'),
        ("@NOT_WIN32_FALSE@", ''),
        ("@PATHSEP@", ';')]

while len(sys.argv) > 2 and '=' in sys.argv[1]:
    arg = sys.argv[1]
    i = arg.find('=')
    subs.append(('@'+arg[:i]+'@', arg[i+1:]))
    del sys.argv[1]

for key, val in subs[:]:
    subs.insert(0, ('@X'+key[1:], val))
    subs.insert(0, ('@Q'+key[1:], val.replace('\\', r'\\')))
    subs.insert(0, ('@QX'+key[1:], val.replace('\\', r'\\')))

def substitute(line):
    for (p,v) in subs:
        line = line.replace(p, v)
    return line

for line in fileinput.input(sys.argv[1]):
    sys.stdout.write(substitute(line))
