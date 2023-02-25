# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.

import re
import os

macrore = r'^[ \t]*#[ \t]*define[ \t]+(?P<name>\w+)(?:\((?P<args>[^()]*)\))?[ \t]*(?P<repl>.*)'
includere = r'^[ \t]*#[ \t]*include[ \t]+"(?P<file>[^"]*)".*'
ifre = r'^[ \t]*#[ \t]*if(n?def)?\b.*'
elifre = r'^[ \t]*#[ \t]*elif\b.*'
elsere = r'^[ \t]*#[ \t]*else\b.*'
endifre = r'^[ \t]*#[ \t]*endif\b.*'
undefre = r'^[ \t]*#[ \t]*undef[ \t]+(?P<uname>\w+).*'
linere = r'^[^()\n]*(?:\([^()]*(?:\([^()]*(?:\([^()]*\)[^()]*)*\)[^()]*)*\)[^()\n]*)*$'

searchre = re.compile(r'(?P<define>'+macrore+r')|(?P<include>'+includere+r')|(?P<if>'+ifre+r')|(?P<elif>'+elifre+r')|(?P<else>'+elsere+r')|(?P<endif>'+endifre+r')|(?P<undef>'+undefre+')|(?P<line>'+linere+r')', re.M)

# comments (/* ... */ where ... is as short as possible)
commentre = re.compile(r'/\*[^*]*(\*(?=[^/])[^*]*)*\*/|//.*')
# horizontal white space
horspcre = re.compile(r'[ \t]+')

# identifier
identre = re.compile(r'\b(?P<ident>[a-zA-Z_]\w*)\b')
nested = r'' # r'(?:\([^()]*(?:\([^()]*(?:\([^()]*(?:\([^()]*(?:\([^()]*(?:\([^()]*\)[^()]*)*\)[^()]*)*\)[^()]*)*\)[^()]*)*\)[^()]*)*\)[^()]*)*'

def process(line, funmac, macros, infunmac=False):
    nline = ''
    pos = 0
    res = identre.search(line)
    while res is not None:
        name = res.group('ident')
        # hack for declaration of BBPreadBBPline with embedded #ifdef/#endif
        if name in ('ifdef', 'endif') and line[res.start(0)-1:res.start(0)] == '#':
            nline += line[pos:res.start(0)-2]
            pos = line.index('\n', res.end(0))
        elif name in macros:
            macros2 = macros.copy()
            del macros2[name]
            repl = process(macros[name], funmac, macros2)
            if line[res.start(0)-1:res.start(0)] == '#' and \
               line[res.start(0)-2:res.start(0)] != '##':
                nline += line[pos:res.start(0)-1]
                nline += '"' + repl.replace('\\', r'\\').replace('"', r'\"') + '"'
            else:
                nline += line[pos:res.start(0)] + repl
            pos = res.end(0)
        elif name in funmac:
            args, repl = funmac[name]
            pat = r'\s*\('
            sep = r''
            for arg in args:
                pat += sep + r'([^,()]*(?:\([^()]*'+nested+r'\)[^,()]*)*)'
                sep = r','
            pat += r'\s*\)'
            r = re.compile(pat)
            res2 = r.match(line, pos=res.end(0))
            if res2 is not None:
                macros2 = {}
                i = 1
                for arg in args:
                    macros2[arg] = res2.group(i).strip()
                    i += 1
                repl = process(repl, {}, macros2, True)
                funmac2 = funmac.copy()
                del funmac2[name]
                repl = process(repl, funmac2, macros)
                repl = repl.replace('##', '')
                nline += line[pos:res.start(0)] + repl
                pos = res2.end(0)
            else:
                nline += line[pos:res.end(0)]
                pos = res.end(0)
        else:
            nline += line[pos:res.end(0)]
            pos = res.end(0)
        res = identre.search(line, pos=pos)
    nline += line[pos:]
    return nline

def readfile(f, funmac=None, macros=None, files=None, printdef=False, include=False):
    data = open(f).read()
    dirname, f = os.path.split(f)
    data = commentre.sub(' ', data)
    data = data.replace('\\\n', '')
    data = horspcre.sub(' ', data)
    if funmac is None:
        funmac = {}
    if macros is None:
        macros = {}
    if files is None:
        files = set()
    files.add(f)
    ndata = []
    skip = []
    res = searchre.search(data, 0)
    while res is not None and res.start(0) < len(data):
        line = res.group()
        if res.group('endif'):
            if printdef:
                ndata.append(line)
            if skip:
                del skip[-1]
        elif res.group('if'):
            if printdef:
                ndata.append(line)
            skip.append(False)
        elif res.group('elif') or res.group('else'):
            if printdef:
                ndata.append(line)
            if include and skip:
                skip[-1] = True
        elif skip and skip[-1]:
            if printdef:
                ndata.append(line)
        elif res.group('define'):
            if printdef:
                ndata.append(line)
            name = res.group('name')
            args = res.group('args')
            repl = res.group('repl')
            if args:
                args = tuple([x.strip() for x in args.split(',')])
                if len(args) == 1 and args[0] == '':
                    args = ()   # empty argument list
                funmac[name] = (args, repl)
            elif include:
                macros[name] = repl
        elif res.group('include'):
            fn = res.group('file')
            if include and '/' not in fn and os.path.exists(os.path.join(dirname, fn)) and fn not in files:
                incdata = readfile(os.path.join(dirname, fn), funmac, macros, files, printdef, include)
                ndata.extend(incdata)
            else:
                ndata.append(line)
        elif res.group('undef'):
            name = res.group('uname')
            if name in macros:
                del macros[name]
            if name in funmac:
                del funmac[name]
        elif res.group('line'):
            line = process(line, funmac, macros)
            ndata.append(line)
        res = searchre.search(data, res.end(0) + 1)
    files.remove(f)
    return ndata

def preprocess(f, printdef=False, include=True):
    return '\n'.join(readfile(f, printdef=printdef, include=include))

# some regexps helping to normalize a declaration
spcre = re.compile(r'\s+')
strre = re.compile(r'([^ *])\*')
comre = re.compile(r',\s*')

def normalize(decl):
    decl = spcre.sub(' ', decl) \
                .replace(' ;', ';') \
                .replace(' (', '(') \
                .replace('( ', '(') \
                .replace(' )', ')') \
                .replace(') ', ')') \
                .replace('* ', '*') \
                .replace(' ,', ',') \
                .replace(')__attribute__', ') __attribute__')
    decl = strre.sub(r'\1 *', decl)
    decl = comre.sub(', ', decl)
    decl = decl.replace('( *', ' (*').replace('* (*', '*(*')
    return decl

if __name__ == '__main__':
    import sys
    for f in sys.argv[1:]:
        print(preprocess(f, printdef=True, include=False))
