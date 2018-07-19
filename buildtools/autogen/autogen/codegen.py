# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.

from __future__ import print_function

import string
import re
import fileinput
import os
import shelve
import sys

from tokenize import tokenize
from tokenize import NL
from filesplit import split_filename


# direct rules
code_gen = {'y':        [ '.tab.c', '.tab.h' ],
            'tab.c':    [ '.tab.o' ],
            'l':        [ '.yy.c', '.yy.h' ],
            'yy.c':     [ '.yy.o' ],
            'mt':       [ '.symbols.h', '.c' ],
            'brg':      [ '.c' ],
            't':        [ '.c' ],
            'c':        [ '.o' ],
            'cpp':      [ '.o' ],
#            'java':     [ '.class' ],
            #'tex':      [ '.html', '.dvi', '.pdf' ],
            #'dvi':      [ '.ps' ],
            #'fig':      [ '.eps' ],
            #'feps':     [ '.eps' ],
            'in':       [ '' ],
            '1.in':     [ '.1' ],  # TODO: add more manpage sections as needed
            'cfg.in':   [ '.cfg' ],
            'java.in':  [ '.java' ],
            'mal.in':   [ '.mal' ],
            'py.in':    [ '.py' ],
            'pl.in':    [ '.pl' ],
            'bat.in':   [ '.bat' ],
            'mt.sed':   [ '.mt' ],
            'c.sed':    [ '.c' ],
            'c.in':     [ '.c' ],
            'h.sed':    [ '.h' ],
            'xsl.in':   [ '.xsl' ],
            'pc.in':    [ '.pc' ],
            'ini.in':   [ '.ini' ],
            'rc':       [ '.res' ],
            'syms':     [ '.def' ],
}


lib_code_gen = { }

bin_code_gen = { }

c_inc = '^[ \t]*#[ \t]*include[ \t]*[<"](?P<fnd>[-a-zA-Z0-9._/]+)[>"]'
xsl_inc = "^[ \t]*<xsl:(include|import)[ \t]+href=['\"](?P<fnd>[a-zA-Z0-9._]+)['\"]"
tex_inc = r"\\epsf(file|box){(?P<fnd>[a-zA-Z0-9._]+)"
t_inc = '^[ \t]*\%[ \t]*include[ \t]*{[ \t](?P<fnd>[-a-zA-Z0-9._/]+)[ \t]*}'

c_inc = re.compile(c_inc, re.MULTILINE)
xsl_inc = re.compile(xsl_inc, re.MULTILINE)
tex_inc = re.compile(tex_inc)
t_inc = re.compile(t_inc, re.MULTILINE)

scan_map = {
    'c': [ c_inc, None, '' ],
    'cpp': [ c_inc, None, '' ],
    'h': [ c_inc, None, '' ],
    'y': [ c_inc, None, '' ],
    'l': [ c_inc, None, '' ],
    'mt': [ c_inc, None, '' ],
    'brg': [ c_inc, None, '' ],
    't': [ t_inc, None, '' ],
    'xsl': [ xsl_inc, None, '' ],
    'tex': [ tex_inc, None, '' ],
}

def readfile(f):
    src = open(f, 'r')
    buf = src.read()
    src.close()
    return buf

def readfilepart(f,ext):
    dir,file = os.path.split(f)
    fn,fext = split_filename(file)
    src = open(f, 'r')
    buf = src.read()
    src.close()
    return buf

# targets are all objects which can somehow be created.
# In the code extraction phase targets are created from files
# possibly containing multiple targets

# In the code generation phase targets are generated using input files
# depending on the extention a target is generated
#
# The do_code_gen also tracks the input files for the code extraction
# of these targets , ie. the dependencies.
def do_code_gen(targets, deps, code_map):
    changes = 1
    while changes:
        ntargets = []
        changes = 0
        for f in targets:
            base,ext = split_filename(f)
            if ext in code_map:
                changes = 1
                for newext in code_map[ext]:
                    newtarget = base + newext
                    if newtarget not in ntargets:
                        ntargets.append(newtarget)
                    if newtarget in deps:
                        if (f not in deps[newtarget]):
                            deps[newtarget].append(f)
                    else:
                        deps[newtarget] = [ f ]
            elif f not in ntargets:
                ntargets.append(f)
        targets = ntargets
    return targets

def find_org(deps,f):
    org = f
    while org in deps:
        org = deps[org][0] #gen code is done first, other deps are appended
    return org

# do_deps finds the dependencies for the given list of targets
# based on the includes (or alike)
#
# incs contains the includes of the local directory, and will be stored
# in a .incs.ag file. And after translating to the install include dirs
# also in .incs.in

buildincsfiles = {}
installincsfiles = {}

# replacement for os.path.join which also normalizes the resultant
# path, keeping Make variables in place
def normpathjoin(a, *p):
    f = os.path.join(a, *p)
    parts = re.split(r'(\$(?:[^()]|\([^()]*\)))', f)
    for i in range(len(parts)):
        prt = parts[i]
        if prt and prt != os.sep and not prt.startswith('$'):
            s, e = 0, len(prt)
            if prt.startswith(os.sep):
                s += len(os.sep)
            if prt.endswith(os.sep):
                e -= len(os.sep)
            parts[i] = prt[:s] + os.path.normpath(prt[s:e]) + prt[e:]
    return ''.join(parts)

def do_deps(targets,deps,includes,incmap,cwd,incdirsmap):
    basename = os.path.basename(cwd)
    incs = {}
    do_scan(targets,deps,incmap,cwd,incs)
    do_dep_combine(deps,includes,cwd,incs)

    normcwd = normpathjoin(cwd)
    buildincs = buildincsfiles[normcwd] = {}
    for k,vals in incs.items():
        buildincs[k] = vals

    installincs = installincsfiles[normcwd] = {}
    for k,vals in incs.items():
        nvals = []
        for i in vals:
            if os.path.isabs(i):
                nvals.append(i)
            else:
                inc = normpathjoin(cwd,i)
                mlen = 0
                subsrc = ''
                subins = ''
                for src,install in incdirsmap:
                    slen = len(src)
                    if slen > mlen and inc.startswith(src):
                        mlen = slen
                        subsrc = src
                        subins = install

                if mlen > 0:
                    inc.replace(subsrc, subins.replace('includedir', '..'))
                nvals.append(inc)
        installincs[k] = nvals

def do_recursive_combine(deplist,includes,incs,depfiles):
    for d in deplist:
        if d in includes:
            for f in includes[d]:
                if f not in depfiles:
                    depfiles.append(f)
                    do_recursive_combine([f],includes,incs,depfiles)
            # need to add include d too
            if d not in depfiles:
                depfiles.append(d)
        elif d in incs:
            if d not in depfiles:
                depfiles.append(d)
                do_recursive_combine(incs[d],includes,incs,depfiles)

# combine the found dependencies, ie. transitive closure.
def do_dep_combine(deps,includes,cwd,incs):
    for target,depfiles in deps.items():
        for d in depfiles:
            if d in incs:
                do_recursive_combine(incs[d],includes,incs,depfiles)
        # remove recursive dependencies (target depends somehow on itself)
        if target in depfiles:
            depfiles.remove(target)

# scan for includes and match against the known deps and include map.
def do_scan_target(target,targets,deps,incmap,cwd,incs):
    base,ext = split_filename(target)
    if target not in incs:
        inc_files = []
        if ext in scan_map:
            org = normpathjoin(cwd,find_org(deps,target))
            if os.path.exists(org):
                b = readfilepart(org,ext)
                pat,sep,incext = scan_map[ext]
                res = pat.search(b)
                while res is not None:
                    p, e = res.span('fnd')
                    if sep is not None:
                        ressep = sep.search(b, p, e)
                        while ressep is not None:
                            n = ressep.start(0)
                            fnd1 = b[p:n] + incext
                            p = ressep.end(0) # start of next file
                            if fnd1 in deps or fnd1 in targets:
                                if fnd1 not in inc_files:
                                    inc_files.append(fnd1)
                            elif fnd1 in incmap:
                                if fnd1 not in inc_files:
                                    inc_files.append(normpathjoin(incmap[fnd1],fnd1))
                            ressep = sep.search(b,p,e)
                    fnd = b[p:e] + incext
                    if fnd in deps or fnd in targets:
                        if fnd not in inc_files:
                            inc_files.append(fnd)
                    elif fnd in incmap:
                        if fnd not in inc_files:
                            inc_files.append(normpathjoin(incmap[fnd],fnd))
                    elif os.path.exists(os.path.join(cwd, fnd)):
                        if fnd not in inc_files:
                            inc_files.append(fnd)
                        if fnd not in incs:
                            incs[fnd] = []
##                     else:
##                         print(fnd + " not in deps or incmap")
                    res = pat.search(b,res.end(0))
        incs[target] = inc_files

def do_scan(targets,deps,incmap,cwd,incs):
    for target in targets:
        if target not in deps:
            do_scan_target(target,targets,{},incmap,cwd,incs)
    for target,depfiles in deps.items():
        do_scan_target(target,targets,deps,incmap,cwd,incs)
        for target in depfiles:
            do_scan_target(target,targets,deps,incmap,cwd,incs)
    #for i in incs.keys():
        #print(i,incs[i])

def expand_env(i):
    if i[0] == '$' and i[1] in ('(','{'):
        sep = '}'
        if i[1] == '(':
            sep = ')'
        var, rest = i[2:].split(sep)

        if var in os.environ:
            value = os.environ[var]
            value = value.replace('{', '(').replace('}', ')')
            return value + rest
    return None

def expand_incdir(i,topdir):
    if i[0:2] == "-I":
        i = i[2:]
    incdir = expand_env(i)
    if (incdir != None):
        return incdir
    dir = i
    if i.find(os.sep) >= 0:
        d,rest = i.split(os.sep, 1)
        if d == "top_srcdir" or d == "top_builddir":
            dir = normpathjoin(topdir, rest)
        elif d == "srcdir" or d == "builddir":
            dir = rest
    return dir

def expand_includes(i,topdir):
    if i[0:2] == "-I":
        i = i[2:]
    if os.path.isabs(i):
        print("!WARNING: it's not portable to use absolute paths: " + i)
    incdir = expand_env(i)
    if (incdir != None):
        incs = incdir.split()
        if (len(incs) > 1):
            return expand_incdirs(incs,topdir)
        else:
            return [(expand_incdir(incdir,topdir),i)]
    dir = expand_incdir(i,topdir)
    return [(dir,i)]

def expand_incdirs(incdirs,topdir):
    dirs = []
    for incdir in incdirs:
        incs = incdir.split()
        if (len(incs) > 1):
            dirs.extend(expand_incdirs(incs,topdir))
        else:
            dirs.extend(expand_includes(incs[0],topdir))
    return dirs


def collect_includes(incdirs, cwd, topdir):
    includes = {}
    incmap = {}

    dirs = expand_incdirs( incdirs, topdir )

    for dir,org in dirs:
        if dir.startswith('$'):
            continue
        dir = normpathjoin(cwd, dir)
        if dir in buildincsfiles:
            incs = buildincsfiles[dir]
        elif dir in installincsfiles:
            incs = installincsfiles[dir]
        else:
            incs = None

        if incs is not None:
            for file in incs.keys():
                incfiles = []
                for inc in incs[file]:
                    if not os.path.isabs(inc) and inc[0] != '$':
                        inc = normpathjoin(org,inc)
                    incfiles.append(inc)
                includes[normpathjoin(org,file)] = incfiles
                incmap[file] = org
        else:
            if os.path.exists(dir):
                for inc in os.listdir(dir):
                    includes[normpathjoin(org,inc)] = [ normpathjoin(org,inc) ]
                    incmap[inc] = org

    return includes,incmap

# includes is an association between an include file (full path) and
# the list of files it includes (full path).

# incmap is an association between an include file (basename) to directory
# where the include file can be found.

# deps is an association between an target file and the files it depends on
# these should for portability be relative or given using an environment
# variable

def codegen(tree, cwd, topdir, incdirsmap):
    includes = {}
    incmap = {}
    if 'INCLUDES' in tree:
        includes,incmap = collect_includes(tree["INCLUDES"],cwd, topdir)

    deps = {}
    for i,v in tree.items():
        targets = []
        if type(v) is type({}) and "SOURCES" in v:
            for f in v["SOURCES"]:
                if f not in targets:
                    targets.append(f)
            targets = do_code_gen(targets,deps,code_gen)
            if i[0:4] == "lib_" or i == "LIBS":
                targets = do_code_gen(targets,deps,lib_code_gen)
            if i[0:4] == "bin_" or i == "BINS":
                targets = do_code_gen(targets,deps,bin_code_gen)
            do_deps(targets,deps,includes,incmap,cwd,incdirsmap)
            v["TARGETS"] = targets
            v["DEPS"] = deps

    for i,v in tree.items():
        if type(v) is type({}) and "SOURCES" in v:
            if i[0:4] == "lib_":
                lib = i[4:] + "_LIBS"
                if lib[0] == "_":
                    lib = lib[1:]

# vim: set expandtab ts=4 sw=4:
