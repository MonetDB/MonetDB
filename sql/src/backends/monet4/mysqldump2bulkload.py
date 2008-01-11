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
# Portions created by CWI are Copyright (C) 1997-2008 CWI.
# All Rights Reserved.

import re
import sys
import string
import fileinput

CREATE = 1
INSERT = 2

write = sys.stdout.write
part = -1

def copy_line( line, write = write ):
    m = re.search('(INSERT INTO ([A-Za-z0-9_]*) .*VALUES \()', line)
    if m is not None:
        write("SELECT '%s';\n" % line[m.start(2):m.end(2)])
        write("COPY INTO %s FROM STDIN USING DELIMITERS ',', '\\n';\n\n" %
              line[m.start(2):m.end(2)])
        return line[0:m.end(1)]
    return ""

def create_line( line, write = write ):
    status = CREATE
    if re.search('\).*;', line) is not None:
        write(');\n')
        return 0
    m = re.search('auto_increment', line)
    if m is not None:
        line = line[0:m.start(0)] + line[m.end(0):]
    m = re.search('unsigned', line)
    if m is not None:
        line = line[0:m.start(0)] + line[m.end(0):]
    m = re.search('KEY[\t ]+([A-Za-z0-9_]+)[\t ]+(\(.*)', line)
    if m is not None:
        write("  CONSTRAINT %s UNIQUE %s\n" %
              (line[m.start(1) : m.end(1)], line[m.start(2) : m.end(2)]))
        return status
    write(line)
    return status

def main(write = write):
    status = 0
    base = ""
    l = 0
    cnt = 0
    for line in fileinput.input():
        if status == INSERT:
            if line[0:l] == base:
                if part < 0 or cnt < part:
                    print line[l:line.rfind(')')]
                    cnt = cnt + 1
            else:
                print # empty line to end copy into
                print "COMMIT;"
                cnt = 0
                status = 0

        if status != INSERT and line[0] != '#' and line[0:1] != '--':
            if status == CREATE:
                status = create_line(line)
            elif re.search('DROP TABLE', line) is not None:
                write('--' + line)
            elif re.search('CREATE TABLE', line) is not None:
                status = CREATE
                write(line)
            elif re.search('INSERT INTO', line) is not None:
                base = copy_line(line)
                l = len(base)
                status = INSERT
                print line[l:line.rfind(')')]
            else:
                write(line)

main()
