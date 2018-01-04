#!/usr/bin/env python

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.

#
# autogen scans the makefile.ag
# and generates both the makefile.am and makefile.msc
#

import sys
del sys.path[0]
import fileinput
import string
import re
from autogen import am
from autogen import msc
from autogen.codegen import *
import tokenize
from autogen import var
import os

def isName(t):
    return t not in ("{", "}", "=")

class parser:
    def __init__ (self):
        self.curvar = var.groupvar("top")
        self.top = self.curvar
        self.cnt = 0
        self.state = "defs"
        self.stack = []
        self.curvar.add("TARGETS", {})

    #def parse(self, type, token, (srow, scol), (erow, ecol), line):
    def parse(self, token, srow, line):
        # order is important
        if self.state == "defs" and token == "\n":
            return
        if self.state == "\\" and token == "\n":
            self.state = "="
        elif self.state == "defs" and isName(token):
            if self.curvar != None:
                self.stack.append(self.curvar)
            self.curvar = var.var(token)
            self.state = "var"
        elif self.state == "=" and token == "\\":
            self.state = "\\"
        elif self.state == "var" and token == "=":
            self.state = "="
        elif self.state == "=" and token == "{":
            self.curvar = var.groupvar(self.curvar._name)
            self.state = "defs"
            self.cnt = self.cnt+1
        elif (self.state == "defs" and token == "}") or (self.state == "=" and token == "\n") or (self.state == "var" and token == "\n"):
            last = len(self.stack)-1
            v = self.stack[last]
            del self.stack[last]
            v.add(self.curvar._name, self.curvar._values)
            self.curvar = v
            self.cnt = self.cnt - 1
            self.state = "defs"
        elif self.state == "=" and isName(token):
            if token == '""':
                token = ""
            if token in self.top:
                for i in self.top[token]:
                    self.curvar.append(i)
            else:
                self.curvar.append(token)
        elif self.state == "var" and token != "=":
            print("Missing = " + token, srow)
        else:
            print("error", token, self.state)


def read_makefile(p, cwd):
    lineno = 0
    for line in fileinput.input(os.path.join(cwd, 'Makefile.ag')):
        if line.lstrip()[0:1] != "#":
            for token in line.split():
                p.parse(token, lineno, line)
            p.parse("\n", lineno, line)
        lineno = lineno + 1

automake="1004000"
if len(sys.argv) > 1:
    topdir = sys.argv[1]
    if len(sys.argv) > 2:
        automake = sys.argv[2]
else:
    topdir = os.getcwd()

def expand_subdirs(subdirs):
    res = []
    for subdir in subdirs:
        if "?" in subdir:
            parts = subdir.split("?")
            if len(parts) == 2:
                dirs = parts[1].split(":")
                if len(dirs) > 2:
                    print("!ERROR:syntax error in conditional subdir: " + subdir)
                else:
                    cond = parts[0]
                    for d in dirs:
                        if d.strip() != "":
                            res.append((d, cond))
                        cond = "!" + cond
            else:
                print("!ERROR:syntax error in conditional subdir: " + subdir)
        else:
            res.append((subdir, None))
    return res

# incdirsmap is a map between srcdir and install-include-dir
def main(cwd, topdir, automake, incdirsmap, conditional = ()):
    p = parser()
    read_makefile(p, cwd)
    codegen(p.curvar, cwd, topdir, incdirsmap)
    (InstallList, DocList, OutList) = am.output(p.curvar, cwd, topdir, automake, conditional)
    msc.output(p.curvar, cwd, topdir)
    if 'SUBDIRS' in p.curvar:
        for (dir, cond) in expand_subdirs(p.curvar['SUBDIRS']):
            d = os.path.join(cwd, dir)
            if os.path.exists(d):
                incdirsmap.append((d, os.path.join('includedir', dir)))
                print(d)
                if cond is None:
                    cond = ()
                else:
                    cond = (cond,)
                (deltaInstallList, deltaDocList, deltaOutList) = \
                                   main(d, topdir, automake, incdirsmap, conditional + cond)
                InstallList = InstallList + deltaInstallList
                DocList = DocList + deltaDocList
                OutList = OutList + deltaOutList
                #cmd = "cd " + dir + "; " + sys.argv[0] + " " + topdir
                #os.system (cmd)
    return InstallList, DocList, OutList

InstallListFd = open("install.lst", "w")
DocListFd = open("doc.lst", "w")
(InstallList, DocList, OutList) = main(topdir, topdir, automake, [])
InstallListFd.writelines(InstallList)
InstallListFd.close()
DocListFd.writelines(DocList)
DocListFd.close()

skip = ["conf/stamp-h", "conf/config.h"]
prev = ''

def filter(st):
    global prev
    if st == prev:
        return ''
    prev = st
    if not st in skip:
        return st + '\n'
    return ''

OutList.sort(key=lambda x: x.count(os.sep))
OutList = map(filter, OutList)
OutListFd = open("acout.in", "w")
OutListFd.writelines(OutList)
OutListFd.close()

# Create cheader.text.h
CAPIHeaderOriginal = open('sql/backends/monet5/UDF/capi/cheader.h', 'r');
CAPIHeaderText = open('sql/backends/monet5/UDF/capi/cheader.text.h', 'w+');
CAPIHeaderText.write("// This file was generated automatically through bootstrap.py.\n// Do not edit this file directly.\n")
CAPIHeaderText.write('const char* cheader_header_text = \n');
for line in CAPIHeaderOriginal:
    if len(line.strip()) > 0:
        CAPIHeaderText.write('"' + line.replace("\n", "").replace('"', '\\"').replace('\\', '\\\\') + '\\n"\n');
CAPIHeaderText.write(";\n");
CAPIHeaderOriginal.close();
CAPIHeaderText.close();



