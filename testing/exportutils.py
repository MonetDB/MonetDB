# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.

import re
import os

# macro definition
macrore = re.compile(r'\s*#\s*define\s+'            # #define
                     r'(?P<name>\w+)'               # name being defined
                     r'(?:\((?P<args>[^)]*)\))?\s*' # optional arguments
                     r'(?P<repl>.*)')               # replacement
# include file
inclre = re.compile(r'\s*#\s*include\s+"(?P<file>[^"]*)"')
# comments (/* ... */ where ... is as short as possible)
cmtre = re.compile(r'/\*[^*]*(\*(?=[^/])[^*]*)*\*/|//.*')
# horizontal white space
horspcre = re.compile(r'[ \t]+')
# identifier
identre = re.compile(r'\b(?P<ident>[a-zA-Z_]\w*)\b')
# undef
undefre = re.compile(r'\s*#\s*undef\s+(?P<name>\w+)')
ifre = re.compile(r'\s*#\s*if') # #if or #ifdef
elifre = re.compile(r'\s*#\s*elif')
elsere = re.compile(r'\s*#\s*else')
endifre = re.compile(r'\s*#\s*endif')

nested = r'' # r'(?:\([^()]*(?:\([^()]*(?:\([^()]*(?:\([^()]*(?:\([^()]*(?:\([^()]*\)[^()]*)*\)[^()]*)*\)[^()]*)*\)[^()]*)*\)[^()]*)*\)[^()]*)*'

def process(line, funmac, macros, infunmac=False):
    nline = ''
    pos = 0
    res = identre.search(line)
    while res is not None:
        name = res.group('ident')
        if name in macros:
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
    data = cmtre.sub(' ', data)
    data = data.replace('\\\n', '')
    data = horspcre.sub(' ', data)
    data = data.splitlines()
    if funmac is None:
        funmac = {}
    if macros is None:
        macros = {}
    if files is None:
        files = set()
    files.add(f)
    ndata = []
    skip = []
    for line in data:
        if endifre.match(line) is not None:
            if printdef:
                ndata.append(line)
            if skip:
                del skip[-1]
            continue
        if ifre.match(line) is not None:
            if printdef:
                ndata.append(line)
            skip.append(False)
            continue
        if elifre.match(line) or elsere.match(line):
            if printdef:
                ndata.append(line)
            if include and skip:
                skip[-1] = True
            continue
        if skip and skip[-1]:
            if printdef:
                ndata.append(line)
            continue
        res = macrore.match(line)
        if res is not None:
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
                continue
            if include:
                macros[name] = repl
            continue
        res = inclre.match(line)
        if res is not None:
            fn = res.group('file')
            if include and '/' not in fn and os.path.exists(os.path.join(dirname, fn)) and fn not in files:
                incdata = readfile(os.path.join(dirname, fn), funmac, macros, files, printdef, include)
                ndata.extend(incdata)
                continue
            ndata.append(line)
            continue
        res = undefre.match(line)
        if res is not None:
            name = res.group('name')
            if name in macros:
                del macros[name]
            if name in funmac:
                del funmac[name]
            continue
        line = process(line, funmac, macros)
        ndata.append(line)
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
        print(preprocess(f, printdef=False))
