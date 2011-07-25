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
# Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
# Copyright August 2008-2011 MonetDB B.V.
# All Rights Reserved.

import string
import re
import fileinput
import os
import shelve
from var import *

from tokenize import tokenize, NL
import sys


mx2mal = re.compile("^@mal[ \t\r\n]+", re.MULTILINE)
mx2h = re.compile("^@h[ \t\r\n]+", re.MULTILINE)
mx2c = re.compile("^@c[ \t\r\n]+", re.MULTILINE)
mx2y = re.compile("^@y[ \t\r\n]+", re.MULTILINE)
mx2l = re.compile("^@l[ \t\r\n]+", re.MULTILINE)
mx2cfg = re.compile("^@cfg[ \t\r\n]+", re.MULTILINE)
mx2sql = re.compile("^@sql[ \t\r\n]+", re.MULTILINE)
mx2java = re.compile("^@java[ \t\r\n]+", re.MULTILINE)
mx2xsl = re.compile("^@xsl[ \t\r\n]+", re.MULTILINE)
mx2sh = re.compile("^@sh[ \t\r\n]+", re.MULTILINE)
#mx2tex = re.compile("^@T|-|\+|\*[ \t\r\n]+", re.MULTILINE)
#mx2html = re.compile("^@w[ \t\r\n]+", re.MULTILINE)

e_mx = re.compile('^@[^{}]', re.MULTILINE)

code_extract = { 'mx': [
                  (mx2mal, '.mal'),
                  (mx2c, '.c'),
                  (mx2h, '.h'),
                  (mx2y, '.y'),
                  (mx2l, '.l'),
                  (mx2cfg, '.cfg'),
                  (mx2sql, '.sql'),
                  (mx2java, '.java'),
                  (mx2xsl, '.xsl'),
                  (mx2sh, ''), ],
                  #(mx2tex, '.tex'),
                  #(mx2tex, '.bdy.tex'),
                  #(mx2html, '.html'),
                  #(mx2tex, '.bdy.html'), ],
                'mx.in': [
                  (mx2mal, '.mal'),
                  (mx2c, '.c'),
                  (mx2h, '.h'),
                  (mx2y, '.y'),
                  (mx2l, '.l'),
                  (mx2cfg, '.cfg'),
                  (mx2sql, '.sql'),
                  (mx2java, '.java'),
                  (mx2xsl, '.xsl'),
                  (mx2sh, ''), ]
                  #(mx2tex, '.tex'),
                  #(mx2tex, '.bdy.tex'),
                  #(mx2html, '.html'),
                  #(mx2tex, '.bdy.html'), ]
}
end_code_extract = { 'mx': e_mx, 'mx.in': e_mx }

# direct rules
code_gen = {'y':        [ '.tab.c', '.tab.h' ],
            'tab.c':    [ '.tab.o' ],
            'l':        [ '.yy.c' ],
            'yy.c':     [ '.yy.o' ],
            'mt':       [ '.symbols.h', '.c' ],
            'brg':      [ '.c' ],
            't':        [ '.c' ],
            'c':        [ '.o' ],
#            'java':     [ '.class' ],
            'mx.in':    [ '.mx' ],
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
mx_inc = '^[ \t]*@include[ \t]*(?P<fnd>[-a-zA-Z0-9._/]+)'

c_inc = re.compile(c_inc, re.MULTILINE)
xsl_inc = re.compile(xsl_inc, re.MULTILINE)
tex_inc = re.compile(tex_inc)
t_inc = re.compile(t_inc, re.MULTILINE)
mx_inc = re.compile(mx_inc, re.MULTILINE)

scan_map = {
    'c': [ c_inc, None, '' ],
    'h': [ c_inc, None, '' ],
    'y': [ c_inc, None, '' ],
    'l': [ c_inc, None, '' ],
    'mt': [ c_inc, None, '' ],
    'brg': [ c_inc, None, '' ],
    't': [ t_inc, None, '' ],
    'xsl': [ xsl_inc, None, '' ],
    'tex': [ tex_inc, None, '' ],
    'mx': [ mx_inc, None, '' ],
}

def split_filename(f):
    base = f
    ext = ""
    if string.find(f,".") >= 0:
        return string.split(f,".", 1)
    return base,ext

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
    buf2 = ""
    if ext != 'mx' and code_extract.has_key(fext):
        epat = end_code_extract[fext]
        for pat,newext in code_extract[fext]:
            if newext == "." + ext:
                res = pat.search(buf)
                while res is not None:
                    m = res.start(0)
                    eres = epat.search(buf,res.end(0))
                    if eres is not None:
                        n = eres.start(0)
                        buf2 = buf2 + buf[m:n]
                        res = pat.search(buf,n)
                    else:
                        buf2 = buf2 + buf[m:]
                        res = None
    else:
        return buf
    return buf2

# targets are all objects which can somehow be created.
# In the code extraction phase targets are created from files
# possibly containing multiple targets
#
# The do_code_extract also tracks the files from which to extract
# these targets , ie. the dependencies.
def do_code_extract(f,base,ext, targets, deps, cwd):
    file = os.path.join(cwd,f)
    if os.path.exists(file) and code_extract.has_key(ext):
        b = readfile(file)
        for pat,newext in code_extract[ext]:
            if pat.search(b) is not None:
                extracted = base + newext
                if extracted not in targets:
                    targets.append( extracted )
                deps[extracted] = [ f ]
    elif f not in targets:
        targets.append(f)

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
            if code_map.has_key(ext):
                changes = 1
                for newext in code_map[ext]:
                    newtarget = base + newext
                    if newtarget not in ntargets:
                        ntargets.append(newtarget)
                    if deps.has_key(newtarget):
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
    while deps.has_key(org):
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

def do_deps(targets,deps,includes,incmap,cwd,incdirsmap):
    basename = os.path.basename(cwd)
    incs = {}
    do_scan(targets,deps,incmap,cwd,incs)
    do_dep_combine(deps,includes,cwd,incs)

    normcwd = os.path.normpath(cwd)
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
                inc = os.path.normpath(os.path.join(cwd,i))
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
        if includes.has_key(d):
            for f in includes[d]:
                if f not in depfiles:
                    depfiles.append(f)
                    do_recursive_combine([f],includes,incs,depfiles)
            # need to add include d too
            if d not in depfiles:
                depfiles.append(d)
        elif incs.has_key(d):
            if d not in depfiles:
                depfiles.append(d)
                do_recursive_combine(incs[d],includes,incs,depfiles)

# combine the found dependencies, ie. transitive closure.
def do_dep_combine(deps,includes,cwd,incs):
    for target,depfiles in deps.items():
        for d in depfiles:
            if incs.has_key(d):
                do_recursive_combine(incs[d],includes,incs,depfiles)
        # remove recursive dependencies (target depends somehow on itself)
        if target in depfiles:
            depfiles.remove(target)

# scan for includes and match against the known deps and include map.
def do_scan_target(target,targets,deps,incmap,cwd,incs):
    base,ext = split_filename(target)
    if not incs.has_key(target):
        inc_files = []
        if scan_map.has_key(ext):
            org = os.path.join(cwd,find_org(deps,target))
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
                            fnd1 = b[p:n]
                            p = ressep.end(0) # start of next file
                            if deps.has_key(fnd1+incext) or fnd1+incext in targets:
                                if fnd1+incext not in inc_files:
                                    inc_files.append(fnd1+incext)
                            elif incmap.has_key(fnd1+incext):
                                if fnd1+incext not in inc_files:
                                    inc_files.append(os.path.join(incmap[fnd1+incext],fnd1+incext))
                            ressep = sep.search(b,p,e)
                    fnd = b[p:e]
                    if deps.has_key(fnd+incext) or fnd+incext in targets:
                        if fnd+incext not in inc_files:
                            inc_files.append(fnd+incext)
                    elif incmap.has_key(fnd+incext):
                        if fnd+incext not in inc_files:
                            inc_files.append(os.path.join(incmap[fnd+incext],fnd+incext))
                    elif os.path.exists(os.path.join(cwd, fnd+incext)):
                        if fnd+incext not in inc_files:
                            inc_files.append(fnd+incext)
                        if not incs.has_key(fnd+incext):
                            incs[fnd+incext] = []
##                     else:
##                         print fnd + incext + " not in deps or incmap"
                    res = pat.search(b,res.end(0))
        incs[target] = inc_files

def do_scan(targets,deps,incmap,cwd,incs):
    for target in targets:
        if not deps.has_key(target):
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
        var, rest = string.split(i[2:], sep)

        if os.environ.has_key( var ):
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
    if string.find(i,os.sep) >= 0:
        d,rest = string.split(i,os.sep, 1)
        if d == "top_srcdir" or d == "top_builddir":
            dir = os.path.join(topdir, rest)
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
        incs = string.split(incdir)
        if (len(incs) > 1):
            return expand_incdirs(incs,topdir)
        else:
            return [(expand_incdir(incdir,topdir),i)]
    dir = expand_incdir(i,topdir)
    return [(dir,i)]

def expand_incdirs(incdirs,topdir):
    dirs = []
    for incdir in incdirs:
        incs = string.split(incdir)
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
        dir = os.path.normpath(os.path.join(cwd, dir))
        if buildincsfiles.has_key(dir):
            incs = buildincsfiles[dir]
        elif installincsfiles.has_key(dir):
            incs = installincsfiles[dir]
        else:
            incs = None

        if incs is not None:
            for file in incs.keys():
                incfiles = []
                for inc in incs[file]:
                    if not os.path.isabs(inc) and inc[0] != '$':
                        inc = os.path.join(org,inc)
                    incfiles.append(inc)
                includes[os.path.join(org,file)] = incfiles
                incmap[file] = org
        else:
            if os.path.exists(dir):
                for inc in os.listdir(dir):
                    includes[os.path.join(org,inc)] = [ os.path.join(org,inc) ]
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
    if tree.has_key("INCLUDES"):
        includes,incmap = collect_includes(tree["INCLUDES"],cwd, topdir)

    deps = {}
    for i,v in tree.items():
        targets = []
        if type(v) is type({}) and v.has_key("SOURCES"):
            for f in v["SOURCES"]:
                base,ext = split_filename(f)
                do_code_extract(f,base,ext, targets, deps, cwd)
            targets = do_code_gen(targets,deps,code_gen)
            if i[0:4] == "lib_" or i == "LIBS":
                targets = do_code_gen(targets,deps,lib_code_gen)
            if i[0:4] == "bin_" or i == "BINS":
                targets = do_code_gen(targets,deps,bin_code_gen)
            do_deps(targets,deps,includes,incmap,cwd,incdirsmap)
            v["TARGETS"] = targets
            v["DEPS"] = deps

    for i,v in tree.items():
        if type(v) is type({}) and v.has_key("SOURCES"):
            if i[0:4] == "lib_":
                lib = i[4:] + "_LIBS"
                if lib[0] == "_":
                    lib = lib[1:]
