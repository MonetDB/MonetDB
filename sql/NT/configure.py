#!/usr/bin/env python

# The contents of this file are subject to the MonetDB Public
# License Version 1.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of
# the License at
# http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
#
# Software distributed under the License is distributed on an "AS
# IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
# implied. See the License for the specific language governing
# rights and limitations under the License.
#
# The Original Code is the Monet Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-2004 CWI.
# All Rights Reserved.

import sys
import fileinput
import os
import string

build=os.path.abspath(sys.argv[1]);
prefix=os.path.abspath(sys.argv[2]);

source=os.path.abspath(os.path.join(build,os.pardir))

# double back slashes
Qprefix = string.replace(prefix, '\\', '\\\\')
Qbuild  = string.replace(build,  '\\', '\\\\')
Qsource = string.replace(source, '\\', '\\\\')

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
    ('@Qexec_prefix@',      "@Qprefix@"),
    ('@Qsysconfdir@',       "@Qprefix@@QDIRSEP@etc"),
    ('@Qlocalstatedir@',    "@Qprefix@@QDIRSEP@var"),
    ('@Qlibdir@',           "@Qprefix@@QDIRSEP@lib"),
    ('@Qbindir@',           "@Qprefix@@QDIRSEP@bin"),
    ('@Qmandir@',           "@Qprefix@@QDIRSEP@man"),
    ('@Qincludedir@',       "@Qprefix@@QDIRSEP@include"),
    ('@Qdatadir@',          "@Qprefix@@QDIRSEP@share"),
    ('@Qinfodir@',          "@Qprefix@@QDIRSEP@info"),
    ('@Qlibexecdir@',       "@Qprefix@@QDIRSEP@libexec"),
    ('@PACKAGE@',           "sql"),
    ('@VERSION@',           "2.4.1"),
    ('@DIRSEP@',            "\\"),
    ('@prefix@',            prefix),
    ('@MONETDB_BUILD@',       build),
    ('@MONETDB_SOURCE@',      source),
    ('@MONETDB_PREFIX@',      os.getenv('MONETDB_PREFIX')),
    ('@QDIRSEP@',           "\\\\"),
    ('@Qprefix@',           Qprefix),
    ('@QMONETDB_BUILD@',      Qbuild),
    ('@QMONETDB_SOURCE@',     Qsource),
    ('@SQL_BACKEND@',       "monet4"),
# values needed for the JDBC driver;
# keep in sync with src/jdbc/build.properties and configure.ag !
    ('@JDBC_MAJOR@',        '1'),
    ('@JDBC_MINOR@',        '0'),
    ('@JDBC_VER_SUFFIX@',   'Dawn'),
    ('@JDBC_DEF_PORT@',     '45123'),
    ('@JDBC_DEF_BLOCKMODE@','true'),
]


def substitute(line):
    for (p,v) in subs:
        line = string.replace(line, p, v);
    return line

for line in fileinput.input(sys.argv[3:]):
    sys.stdout.write(substitute(line))
