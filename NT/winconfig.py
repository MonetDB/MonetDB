#!/usr/bin/env python

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.

import sys
import fileinput
import os
import subprocess

subs = [("@exec_prefix@", r'%prefix%'),
        ("@bindir@", r'%exec_prefix%\bin'),
        ("@sbindir@", r'%exec_prefix%\sbin'),
        ("@libexecdir@", r'%exec_prefix%\libexec'),
        ("@datadir@", r'%prefix%\share'),
        ("@datarootdir@", r'%prefix%\share'),
        ("@sysconfdir@", r'%prefix%\etc'),
        ("@sharedstatedir@", r'%prefix%\com'),
        ("@localstatedir@", r'%prefix%\var'),
        ("@libdir@", r'%exec_prefix%\lib'),
        ("@infodir@", r'%prefix%\info'),
        ("@mandir@", r'%prefix%\man'),
        ("@includedir@", r'%prefix%\include'),
        ("@oldincludedir@", r'\usr\include'),
        ("@pkgdatadir@", r'%prefix%\share\@PACKAGE@'),
        ("@pkglibdir@", r'%exec_prefix%\lib\@PACKAGE@'),
        ("@pkgincludedir@", r'%prefix%\include\@PACKAGE@'),
        ("@DIRSEP@", '\\'),
        ("@PATHSEP@", ';')]

if len(sys.argv) > 1 and sys.argv[1].endswith(r'\winconfig_conds.py'):
    conds = {}
    for line in fileinput.input(sys.argv[1]):
        exec(line, None, conds)
    for k in conds.keys():
        subs.append(('@'+k+'@', conds[k]))
    del sys.argv[1]

while len(sys.argv) > 2 and '=' in sys.argv[1]:
    arg = sys.argv[1]
    i = arg.find('=')
    subs.append(('@'+arg[:i]+'@', arg[i+1:]))
    del sys.argv[1]
    if arg[:i] == 'TOPDIR':
        subs.append(('@BUILD@', os.path.abspath(arg[i+1:])))

subs.append(('@SOURCE@', os.path.abspath(os.path.dirname(os.path.dirname(sys.argv[0])))))

for key, val in subs[:]:
    # X prefix for execution-time value
    subs.insert(0, ('@X'+key[1:], val))
    # Q prefix for quoted value (i.e. \ needs to be scaped)
    subs.insert(0, ('@Q'+key[1:], val.replace('\\', r'\\')))
    # QX prefix for quoted execution-time value
    subs.insert(0, ('@QX'+key[1:], val.replace('\\', r'\\')))

def substitute(line):
    for (p,v) in subs:
        line = line.replace(p, v)
    return line

for line in fileinput.input(sys.argv[1]):
    sys.stdout.write(substitute(line))
