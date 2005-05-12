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
# Portions created by CWI are Copyright (C) 1997-2005 CWI.
# All Rights Reserved.

import string
import os
import re

# the text that is put at the top of every generated Makefile.msc
MAKEFILE_HEAD = '''
## Use: nmake -f makefile.msc install

# Nothing much configurable below

# cl -help describes the options
!IFDEF USE_MINGW
CC = gcc -mwin32 -mno-cygwin -mthreads -std=gnu99 -g
CXX = g++ -mwin32 -mno-cygwin -g
ARCHIVER = ar cr
GENDLL = -mdll
!ELSE
!IFDEF DEBUG
CC = cl -GF -W3 -wd4273 -wd4102 -MDd -nologo -Zi -G6 -Od -D_DEBUG -RTC1 -ZI
!ELSE
CC = cl -GF -W3 -wd4273 -wd4102 -MD -nologo -Zi -G6
!ENDIF
CXX = $(CC)
ARCHIVER = lib
GENDLL =
!ENDIF
# optimize use -Ox
RC = rc

JAVAC = javac
JAR = jar

CFLAGS = -I. -I$(TOPDIR) $(LIBC_INCS) $(INCLUDES)

# No general LDFLAGS needed
!IFDEF USE_MINGW
LDFLAGS =
OUTPUTEXE = -o 
OUTPUTOBJ = -o 
OUTPUTLIB =
LDFLAGS2 =
O = o
CXXFLAGS = $(CFLAGS)
!ELSE
LDFLAGS = /link
OUTPUTEXE = -Fe
OUTPUTOBJ = -Fo
OUTPUTLIB = /out:
LDFLAGS2 = /subsystem:console /NODEFAULTLIB:LIBC
O = obj
CXXFLAGS = $(CFLAGS) -EHsc
!ENDIF
INSTALL = copy
# TODO
# replace this hack by something like configure ...
MKDIR = mkdir
ECHO = echo
CD = cd

CXXEXT = \\\"cxx\\\"

'''

#automake_ext = ['c', 'cc', 'h', 'y', 'yy', 'l', 'll', 'glue.c']
automake_ext = ['c', 'cc', 'h', 'tab.c', 'tab.cc', 'tab.h', 'yy.c', 'yy.cc', 'glue.c', 'proto.h', 'py.i', 'pm.i', '']

def split_filename(f):
    base = f
    ext = ""
    if string.find(f, ".") >= 0:
        return string.split(f, ".", 1)
    return base, ext

def rsplit_filename(f):
    base = f
    ext = ""
    s = string.rfind(f, ".")
    if s >= 0:
        return f[:s], f[s+1:]
    return base, ext

def msc_basename(f):
    # return basename (i.e. just the file name part) of a path, no
    # matter which directory separator was used
    return string.split(string.split(f, '/')[-1], '\\')[-1]

def msc_dummy(fd, var, values, msc):
    res = fd

def msc_list2string(l, pre, post):
    res = ""
    for i in l:
        res = res + pre + i + post
    return res

def create_dir(fd, v,n):
    # Stupid Windows/nmake cannot cope with single-letter directory
    # names; apparently, it treats them as drive-letters, unless we
    # explicitely call them ".\?".
    if len(v) == 1:
        vv = '.\\%s' % v
    else:
        vv = v
    fd.write('%s-all: "%s-dir" "%s-Makefile"\n' % (n, n, n))
    fd.write('\t$(CD) "%s" && $(MAKE) /nologo "prefix=$(prefix)" all \n' % vv)
    fd.write('%s-dir: \n\tif not exist "%s" $(MKDIR) "%s"\n' % (n, vv, vv))
    fd.write('%s-Makefile: "$(SRCDIR)\\%s\\Makefile.msc"\n' % (n, v))
    fd.write('\t$(INSTALL) "$(SRCDIR)\\%s\\Makefile.msc" "%s\\Makefile"\n' % (v, v))
    fd.write('%s-check: "%s"\n' % (n, vv))
    fd.write('\t$(CD) "%s" && $(MAKE) /nologo "prefix=$(prefix)" check\n' % vv)

    fd.write('%s-install: "$(bindir)" "$(libdir)"\n' % n)
    fd.write('\t$(CD) "%s" && $(MAKE) /nologo "prefix=$(prefix)" install\n' % vv)

def empty_dir(fd, n):

    fd.write('%s-all:\n' % (n))
    fd.write('%s-check:\n' % (n))
    fd.write('%s-install:\n' % (n))

def create_subdir(fd, dir):
    res = ""
    if string.find(dir, "?") > -1:
        parts = string.split(dir, "?")
        if len(parts) == 2:
            dirs = string.split(parts[1], ":")
            fd.write("!IFDEF %s\n" % parts[0])
            if len(dirs) > 0 and string.strip(dirs[0]) != "":
                create_dir(fd, dirs[0],parts[0])
            else:
                empty_dir(fd, parts[0])
            if len(dirs) > 1 and string.strip(dirs[1]) != "":
                fd.write("!ELSE\n")
                create_dir(fd, dirs[1],parts[0])
            else:
                fd.write("!ELSE\n")
                empty_dir(fd, parts[0])
            fd.write("!ENDIF\n")
        res = parts[0]
    else:
        create_dir(fd, dir,dir)
        res = dir
    return res

def msc_subdirs(fd, var, values, msc):
    # to cope with conditional subdirs:
    dirs = []
    i = 0
    nvalues = []
    for dir in values:
        i = i + 1
        val = create_subdir(fd, dir)
        if val:
            nvalues.append(val)

    fd.write("all-recursive: %s\n" % msc_list2string(nvalues, '"', '-all" '))
    fd.write("check-recursive: %s\n" % msc_list2string(nvalues, '"', '-check" '))
    fd.write("install-recursive: %s\n" % msc_list2string(nvalues, '"', '-install" '))

def msc_assignment(fd, var, values, msc):
    o = ""
    for v in values:
        o = o + " " + string.replace(v, '/', '\\')
    if var[0] != '@':
        fd.write("%s = %s\n" % (var, o))

def msc_cflags(fd, var, values, msc):
    o = ""
    for v in values:
        o = o + " " + v
    fd.write("%s = $(%s) %s\n" % (var.replace('-','_'), var.replace('-','_'), o))

def msc_extra_dist(fd, var, values, msc):
    for i in values:
        msc['EXTRA_DIST'].append(i)

def msc_extra_headers(fd, var, values, msc):
    for i in values:
        msc['HDRS'].append(i)

def msc_libdir(fd, var, values, msc):
    msc['LIBDIR'] = values[0]

def msc_mtsafe(fd, var, values, msc):
    fd.write("CFLAGS=$(CFLAGS) $(thread_safe_flag_spec)\n")

def msc_add_srcdir(path, msc, prefix =""):
    dir = path
    if dir[0] == '$':
        return ""
    elif not os.path.isabs(dir):
        dir = "$(SRCDIR)/" + dir
    else:
        return ""
    return prefix+string.replace(dir, '/', '\\')

def msc_translate_dir(path, msc):
    dir = path
    rest = ""
    if string.find(path, '/') >= 0:
        dir, rest = string.split(path, '/', 1)
    if dir == "top_builddir":
        dir = "$(TOPDIR)"
    elif dir == "top_srcdir":
        dir = "$(TOPDIR)/.."
    elif dir == "builddir":
        dir = "."
    elif dir == "srcdir":
        dir = "$(SRCDIR)"
    elif dir in ('bindir', 'builddir', 'datadir', 'includedir', 'infodir',
                 'libdir', 'libexecdir', 'localstatedir', 'mandir',
                 'oldincludedir', 'pkgbindir', 'pkgdatadir', 'pkgincludedir',
                 'pkglibdir', 'pkglocalstatedir', 'pkgsysconfdir', 'sbindir',
                 'sharedstatedir', 'srcdir', 'sysconfdir', 'top_builddir',
                 'top_srcdir'):
        dir = "$("+dir+")"
    if rest:
        dir = dir+ "\\" + rest
    return string.replace(dir, '/', '\\')

def msc_translate_file(path, msc):
    if os.path.isfile(os.path.join(msc['cwd'], path)):
        return "$(SRCDIR)\\" + path
    return path

def msc_space_sep_list(l):
    res = ""
    for i in l:
        res = res + " " + i
    return res + "\n"

def msc_find_srcs(target, deps, msc):
    base, ext = split_filename(target)
    f = target
    pf = f
    while ext != "h" and deps.has_key(f):
        f = deps[f][0]
        b, ext = split_filename(f)
        if ext in automake_ext:
            pf = f
    # built source if has dep and ext != cur ext
    if deps.has_key(pf) and pf not in msc['BUILT_SOURCES']:
        pfb, pfext = split_filename(pf)
        sfb, sfext = split_filename(deps[pf][0])
        if sfext != pfext:
            msc['BUILT_SOURCES'].append(pf)
    return pf

def msc_find_hdrs(target, deps, hdrs):
    base, ext = split_filename(target)
    f = target
    pf = f
    while ext != "h" and deps.has_key(f):
        f = deps[f][0]
        b, ext = split_filename(f)
        if ext in automake_ext:
            pf = f
    return pf

def msc_additional_libs(fd, name, sep, type, list, dlibs, msc, pref = 'lib', dll = '.dll'):
    deps = pref+sep+name+dll+": "
    if type == "BIN":
        add = name.replace('-','_')+"_LIBS ="
    elif type == "LIB":
        add = pref+sep+name.replace('-','_')+"_LIBS ="
    else:
        add = name.replace('-','_') + " ="
    add2 = add
    for l in list:
        if l == "@LIBOBJS@":
            add = add + " $(LIBOBJS)"
            add2 = add2 + " $(LIBOBJS)"
        elif l[:2] == "-l":
            add = add + " lib"+l[2:]+".lib"
            add2 = add2 + ' "%s"' % l
        elif l[0] == "-":
            add = add + ' "%s"' % l
            add2 = add2 + ' "%s"' % l
        elif l[0] == '$':
            add = add + ' %s' % l
            add2 = add2 + ' %s' % l
        elif l[0] != "@":
            lib = msc_translate_dir(l, msc) + '.lib'
            # add quotes if space in name
            # we can't always add quotes since for some weird reason
            # in src\modules\plain you will then have a problem with
            # lib_algebra.lib.
            if ' ' in lib:
                lib = '"%s"' % lib
            add = add + ' %s' % lib
            add2 = add2 + ' %s' % lib
            deps = deps + ' %s' % lib
    # this can probably be removed...
    for l in dlibs:
        if l == "@LIBOBJS@":
            add = add + " $(LIBOBJS)"
            add2 = add2 + " $(LIBOBJS)"
        elif l[:2] == "-l":
            add = add + " lib"+l[2:]+".lib"
            add2 = add2 + " " + l
        elif l[0] in  ("-", "$"):
            add = add + " " + l
            add2 = add2 + " " + l
        elif l[0] not in  ("@"):
            add = add + " " + msc_translate_dir(l, msc) + ".lib"
            add2 = add2 + " " + msc_translate_dir(l, msc) + ".lib"
            deps = deps + ' "' + msc_translate_dir(l, msc) + '.lib"'
    fd.write('!IFDEF USE_MINGW\n')
    if type != "MOD":
        fd.write(deps + "\n")
    fd.write(add2 + "\n")
    fd.write('!ELSE\n')
    if type != "MOD":
        fd.write(deps + "\n")
    fd.write(add + "\n")
    fd.write('!ENDIF\n')

def msc_translate_ext(f):
    n = string.replace(f, '.o', '.$O')
    return string.replace(n, '.cc', '.cxx')

def msc_find_target(target, msc):
    tree = msc['TREE']
    for t, v in tree.items():
        if type(v) is type({}) and v.has_key('TARGETS'):
            targets = v['TARGETS']
            if target in targets:
                if t == "BINS" or t[0:4] == "bin_":
                    return "BIN", "BIN"
                elif (t[0:4] == "lib_"):
                    return "LIB", string.upper(t[4:])
                elif t == "LIBS":
                    name, ext = split_filename(target)
                    return "LIB", string.upper(name)
    return "UNKNOWN", "UNKNOWN"


def msc_deps(fd, deps, objext, msc):
    if not msc['DEPS']:
        for tar, deplist in deps.items():
            t = msc_translate_ext(tar)
            b, ext = split_filename(t)
            tf = msc_translate_file(t, msc)
            sep = tf + ": "
            _in = []
            for d in deplist:
                if not os.path.isabs(d):
                    dep = msc_translate_dir(msc_translate_ext(msc_translate_file(d, msc)), msc)
                    if dep[-3:] == '.in':
                        if dep not in msc['_IN']:
                            _in.append((d[:-3], dep))
                        dep = d[:-3]
                    if dep != t:
                        fd.write('%s"%s"' % (sep, dep))
                        sep = " "
                else:
                    print "!WARNING: dropped absolute dependency " + d
            if sep == " ":
                fd.write("\n")
                if tf+'.mx.in' in deplist:
                    fd.write('\t$(MX) $(MXFLAGS) -x sh "%s.mx"\n' % tf)
            for x, y in _in:
                # TODO
                # replace this hack by something like configure ...
                fd.write('%s: "%s"\n' % (x, y))
                fd.write('\t$(CONFIGURE) "%s" > "%s"\n' % (y, x))
                msc['_IN'].append(y)
            getsrc = ""
            src = msc_translate_dir(msc_translate_ext(msc_translate_file(deplist[0], msc)), msc)
            if os.path.split(src)[0]:
                getsrc = '\t$(INSTALL) "%s" "%s"\n' % (src, os.path.split(src)[1])
            if ext == "tab.h":
                fd.write(getsrc)
                x, de = split_filename(deplist[0])
                if de == 'y':
                    fd.write('\t$(YACC) $(YFLAGS) "%s.y"\n' % b)
                    fd.write("\t$(DEL) y.tab.c\n")
                    fd.write('\t$(MV) y.tab.h "%s.tab.h"\n' % b)
                else:
                    fd.write('\t$(YACC) $(YFLAGS) "%s.yy"\n' % b)
                    fd.write("\t$(DEL) y.tab.c\n")
                    fd.write('\t$(MV) y.tab.h "%s.tab.h"\n' % b)
            if ext == "tab.c":
                fd.write(getsrc)
                fd.write('\t$(YACC) $(YFLAGS) "%s.y"\n' % b)
                fd.write('\t$(MV) y.tab.c "%s.tab.c"\n' % b)
                fd.write("\t$(DEL) y.tab.h\n")
            if ext == "tab.cxx":
                fd.write(getsrc)
                fd.write('\t$(YACC) $(YFLAGS) "%s.yy"\n' % b)
                fd.write('\t$(MV) y.tab.c "%s.tab.cxx"\n' % b)
                fd.write("\t$(DEL) y.tab.h\n")
            if ext == "yy.c":
                fd.write(getsrc)
                fd.write('\t$(LEX) $(LFLAGS) "%s.l"\n' % b)
                # either lex.yy.c or lex.$(PARSERNAME).c gets generated
                fd.write('\tif exist lex.yy.c $(MV) lex.yy.c "%s.yy.c"\n' % b)
                fd.write('\tif exist lex.$(PARSERNAME).c $(MV) lex.$(PARSERNAME).c "%s.yy.c"\n' % b)
            if ext == "yy.cxx":
                fd.write(getsrc)
                fd.write('\t$(LEX) $(LFLAGS) "%s.ll"\n' % b)
                # either lex.yy.c or lex.$(PARSERNAME).c gets generated
                fd.write('\tif exist lex.yy.c $(MV) lex.yy.c "%s.yy.cxx"\n' % b)
                fd.write('\tif exist lex.$(PARSERNAME).c $(MV) lex.$(PARSERNAME).c "%s.yy.cxx"\n' % b)

            if ext == "glue.c":
                fd.write(getsrc)
                fd.write('\t$(MEL) $(INCLUDES) -o "%s" -glue "%s.m"\n' % (t, b))
            if ext == "proto.h":
                fd.write(getsrc)
                fd.write('\t$(MEL) $(INCLUDES) -o "%s" -proto "%s.m"\n' % (t, b))
            if ext == "mil":
                fd.write(getsrc)
                if b+".tmpmil" in deplist:
                    fd.write('\t$(MEL) $(INCLUDES) -mil "%s.m" > "$@"\n' % (b))
                    fd.write('\ttype "%s.tmpmil" >> "$@"\n' % (b))
            if ext in ("$O", "glue.$O", "tab.$O", "yy.$O"):
                target, name = msc_find_target(tar, msc)
                if name[0] == '_':
                    name = name[1:]
                if target == "LIB":
                    d, dext = split_filename(deplist[0])
                    if dext in ("c", "glue.c", "yy.c", "tab.c"):
                        fd.write('\t$(CC) $(CFLAGS) $(INCLUDES) $(GENDLL) -DLIB%s $(OUTPUTOBJ)"%s" -c "%s"\n' %
                                 (name, t, src))
                    elif dext == "cc":
                        fd.write('\t$(CXX) $(CXXFLAGS) $(INCLUDES) $(GENDLL) -DLIB%s $(OUTPUTOBJ)"%s" -c "%s"\n' %
                                 (name, t, src))
            if ext == 'res':
                fd.write("\t$(RC) -fo%s %s\n" % (t, src))
    msc['DEPS'].append("DONE")

# list of scripts to install
def msc_scripts(fd, var, scripts, msc):

    s, ext = string.split(var, '_', 1);
    ext = [ ext ]
    if scripts.has_key("EXT"):
        ext = scripts["EXT"] # list of extentions

    sd = "SCRIPTSDIR"
    if scripts.has_key("DIR"):
        sd = scripts["DIR"][0] # use first name given
    sd = msc_translate_dir(sd, msc)

    for script in scripts['TARGETS']:
        s,ext2 = rsplit_filename(script)
        if not ext2 in ext:
            continue
        if (script, script, '', sd, '') in msc['INSTALL']:
            continue
        if os.path.isfile(os.path.join(msc['cwd'], script+'.in')):
            inf = '$(SRCDIR)\\%s.in' % script
            if inf not in msc['_IN']:
                # TODO
                # replace this hack by something like configure ...
                fd.write('%s: "%s"\n' % (script, inf))
                fd.write('\t$(CONFIGURE) "%s" > "%s"\n' % (inf, script))
                msc['_IN'].append(inf)
        elif os.path.isfile(os.path.join(msc['cwd'], script)):
            fd.write('%s: "$(SRCDIR)\\%s"\n' % (script, script))
            fd.write('\t$(INSTALL) "$(SRCDIR)\\%s" "%s"\n' % (script, script))
        if script != 'mprof.mil':
            msc['INSTALL'].append((script, script, '', sd, ''))
            msc['SCRIPTS'].append(script)

##    msc_deps(fd, scripts['DEPS'], "\.o", msc)

# list of headers to install
def msc_headers(fd, var, headers, msc):

    sd = "HEADERSDIR"
    if headers.has_key("DIR"):
        sd = headers["DIR"][0] # use first name given
    sd = msc_translate_dir(sd, msc)

    hdrs_ext = headers['HEADERS']
    for header in headers['TARGETS']:
        h, ext = split_filename(header)
        if ext in hdrs_ext:
            if os.path.isfile(os.path.join(msc['cwd'], header+'.in')):
                inf = '$(SRCDIR)\\%s.in' % header
                if inf not in msc['_IN']:
                    # TODO
                    # replace this hack by something like configure ...
                    fd.write('%s: "%s"\n' % (header, inf))
                    fd.write('\t$(CONFIGURE) "%s" > "%s"\n' % (inf, header))
                    msc['_IN'].append(inf)
            else:
                fd.write('%s: "$(SRCDIR)\\%s"\n' % (header, header))
##                fd.write('\t$(INSTALL) "$(SRCDIR)\\%s" "%s"\n' % (header, header))
##                fd.write('\tif not exist "%s" if exist "$(SRCDIR)\\%s" $(INSTALL) "$(SRCDIR)\\%s" "%s"\n' % (header, header, header, header))
                fd.write('\t$(INSTALL) "$(SRCDIR)\\%s" "%s"\n' % (header, header))
            msc['INSTALL'].append((header, header, '', sd, ''))

##    msc_find_ins(msc, headers)
##    msc_deps(fd, headers['DEPS'], "\.o", msc)

def msc_doc(fd, var, docmap, msc):
    docmap['TARGETS']=[]

def msc_binary(fd, var, binmap, msc):

    if type(binmap) == type([]):
        name = var[4:]
        if name == 'SCRIPTS':
            for i in binmap:
                if os.path.isfile(os.path.join(msc['cwd'], i+'.in')):
                    # TODO
                    # replace this hack by something like configure ...
                    fd.write('%s: "$(SRCDIR)\\%s.in"\n' % (i, i))
                    fd.write('\t$(CONFIGURE) "$(SRCDIR)\\%s.in" > "%s"\n' % (i, i))
                else:
                    fd.write('%s: "$(SRCDIR)\\%s"\n' % (i, i))
                    fd.write('\t$(INSTALL) "$(SRCDIR)\\%s" "%s"\n' % (i, i))
                msc['INSTALL'].append((i, i, '', '$(bindir)', ''))
        else: # link
            src = binmap[0][4:]
            fd.write('%s: "%s"\n' % (name, src))
            fd.write('\t$(CP) "%s" "%s"\n\n' % (src, name))
            n_nme, n_ext = split_filename(name)
            s_nme, s_ext = split_filename(src)
            if n_ext  and  s_ext  and  n_ext == s_ext:
                ext = ''
            else:
                ext = '.exe'
            msc['INSTALL'].append((name, src, ext, '$(bindir)', ''))
            msc['SCRIPTS'].append(name)
        return

    HDRS = []
    hdrs_ext = []
    if binmap.has_key('HEADERS'):
        hdrs_ext = binmap['HEADERS']

    SCRIPTS = []
    scripts_ext = []
    if binmap.has_key('SCRIPTS'):
        scripts_ext = binmap['SCRIPTS']

    name = var[4:]
    if binmap.has_key("NAME"):
        binname = binmap['NAME'][0]
    else:
        binname = name

    if binmap.has_key("DIR"):
        bd = binmap["DIR"][0] # use first name given
        fd.write("%sdir = %s\n" % (binname.replace('-','_'), msc_translate_dir(bd,msc)) );
    else:
        fd.write("%sdir = $(bindir)\n" % binname.replace('-','_'));

    msc['BINS'].append(binname)

    if binmap.has_key('MTSAFE'):
        fd.write("CFLAGS=$(CFLAGS) $(thread_safe_flag_spec)\n")

    binlist = []
    if binmap.has_key("LIBS"):
        binlist = binlist + binmap["LIBS"]
    if binmap.has_key("WINLIBS"):
        binlist = binlist + binmap["WINLIBS"]
    if binlist:
        msc_additional_libs(fd, binname, "", "BIN", binlist, [], msc)

    srcs = binname.replace('-','_')+"_OBJS ="
    for target in binmap['TARGETS']:
        t, ext = split_filename(target)
        if ext == "o":
            srcs = srcs + " " + t + ".$O"
        elif ext == "glue.o":
            srcs = srcs + " " + t + ".glue.$O"
        elif ext == "tab.o":
            srcs = srcs + " " + t + ".tab.$O"
        elif ext == "yy.o":
            srcs = srcs + " " + t + ".yy.$O"
        elif ext in hdrs_ext:
            HDRS.append(target)
        elif ext in scripts_ext:
            if target not in SCRIPTS:
                SCRIPTS.append(target)
    fd.write(srcs + "\n")
    fd.write("%s.exe: $(%s_OBJS)\n" % (binname, binname.replace('-','_')))
    is_cxx = None
    if not msc['DEPS']:
        for tar, deplist in binmap['DEPS'].items():
            b, ext = split_filename(tar)
            if ext in ('cc', 'cxx'):
                is_cxx = 1
                break
    if is_cxx:
        fd.write('\t$(CXX) $(CXXFLAGS)')
    else:
        fd.write('\t$(CC) $(CFLAGS)')
    fd.write(" $(OUTPUTEXE)%s.exe $(%s_OBJS) $(LDFLAGS) $(%s_LIBS) $(LDFLAGS2)\n\n" % (binname, binname.replace('-','_'), binname.replace('-','_')))

    if SCRIPTS:
        fd.write(binname.replace('-','_')+"_SCRIPTS =" + msc_space_sep_list(SCRIPTS))
        msc['BUILT_SOURCES'].append("$(" + binname.replace('-','_') + "_SCRIPTS)")

    if binmap.has_key('HEADERS'):
        for h in HDRS:
            msc['HDRS'].append(h)

    msc_deps(fd, binmap['DEPS'], ".$O", msc)

def msc_bins(fd, var, binsmap, msc):

    HDRS = []
    hdrs_ext = []
    if binsmap.has_key('HEADERS'):
        hdrs_ext = binsmap['HEADERS']

    SCRIPTS = []
    scripts_ext = []
    if binsmap.has_key('SCRIPTS'):
        scripts_ext = binsmap['SCRIPTS']

    name = ""
    if binsmap.has_key("NAME"):
        name = binsmap["NAME"][0] # use first name given
    if binsmap.has_key('MTSAFE'):
        fd.write("CFLAGS=$(CFLAGS) $(thread_safe_flag_spec)\n")

    for binsrc in binsmap['SOURCES']:
        bin, ext = split_filename(binsrc)
        if ext not in automake_ext:
            msc['EXTRA_DIST'].append(binsrc)

        is_cxx = ext in ('cc', 'cxx')   # whether source is C++ or C

        if binsmap.has_key("DIR"):
            bd = binsmap["DIR"][0] # use first name given
            fd.write("%sdir = %s\n" % (bin.replace('-','_'), msc_translate_dir(bd,msc)) );
        else:
            fd.write("%sdir = $(bindir)\n" % (bin.replace('-','_')) );

        msc['BINS'].append(bin)

        if binsmap.has_key(bin + "_LIBS"):
            msc_additional_libs(fd, bin, "", "BIN", binsmap[bin + "_LIBS"], [], msc)
        else:
            binslist = []
            if binsmap.has_key("LIBS"):
                binslist = binslist + binsmap["LIBS"]
            if binsmap.has_key("WINLIBS"):
                binslist = binslist + binsmap["WINLIBS"]
            if binslist:
                msc_additional_libs(fd, bin, "", "BIN", binslist, [], msc)

        srcs = bin+"_OBJS ="
        for target in binsmap['TARGETS']:
            t, ext = split_filename(target)
            if t == bin:
                t, ext = split_filename(target)
                if ext == "o":
                    srcs = srcs + " " + t + ".$O"
                elif ext == "glue.o":
                    srcs = srcs + " " + t + ".glue.$O"
                elif ext == "tab.o":
                    srcs = srcs + " " + t + ".tab.$O"
                elif ext == "yy.o":
                    srcs = srcs + " " + t + ".yy.$O"
                elif ext == 'res':
                    srcs = srcs + " " + t + ".res"
                elif ext in hdrs_ext:
                    HDRS.append(target)
                elif ext in scripts_ext:
                    if target not in SCRIPTS:
                        SCRIPTS.append(target)
        fd.write(srcs + "\n")
        fd.write("%s.exe: $(%s_OBJS)\n" % (bin, bin.replace('-','_')))
        if is_cxx:
            fd.write('\t$(CXX) $(CXXFLAGS)')
        else:
            fd.write('\t$(CC) $(CFLAGS)')
        fd.write(" $(OUTPUTEXE)%s.exe $(%s_OBJS) $(LDFLAGS) $(%s_LIBS) $(LDFLAGS2)\n\n" % (bin, bin.replace('-','_'), bin.replace('-','_')))

    if SCRIPTS:
        fd.write(name.replace('-','_')+"_SCRIPTS =" + msc_space_sep_list(SCRIPTS))
        msc['BUILT_SOURCES'].append("$(" + name.replace('-','_') + "_SCRIPTS)")

    if binsmap.has_key('HEADERS'):
        for h in HDRS:
            msc['HDRS'].append(h)

    msc_deps(fd, binsmap['DEPS'], ".$O", msc)

def msc_mods_to_libs(fd, var, modmap, msc):
    modname = var[:-4]+"LIBS"
    msc_assignment(fd, var, modmap, msc)
    msc_additional_libs(fd, modname, "", "MOD", modmap, [], msc)

def msc_library(fd, var, libmap, msc):

    name = var[4:]
    sep = ""
    pref = 'lib'
    dll = '.dll'
    if libmap.has_key("NAME"):
        libname = libmap['NAME'][0]
    else:
        libname = name

    if libmap.has_key('PREFIX'):
        if libmap['PREFIX']:
            pref = libmap['PREFIX'][0]
        else:
            pref = ''
            # if underneath a directory called "python" (up to 3 levels),
            # set DLL suffix to ".pyd"
            h,t = os.path.split(msc['cwd'])
            if t == 'python':
                dll = '.pyd'
            else:
                h,t = os.path.split(h)
                if t == 'python' or os.path.basename(h) == 'python':
                    dll = '.pyd'

    if (libname[0] == "_"):
        sep = "_"
        libname = libname[1:]
    if libmap.has_key('SEP'):
        sep = libmap['SEP'][0]

    lib = "lib"
    ld = "LIBDIR"
    if libmap.has_key("DIR"):
        lib = libname
        ld = libmap["DIR"][0] # use first name given
    ld = msc_translate_dir(ld,msc)

    HDRS = []
    hdrs_ext = []
    if libmap.has_key('HEADERS'):
        hdrs_ext = libmap['HEADERS']

    SCRIPTS = []
    scripts_ext = []
    if libmap.has_key('SCRIPTS'):
        scripts_ext = libmap['SCRIPTS']

    v = sep + libname
    if libmap.has_key('NOINST'):
        msc['NLIBS'].append(pref + v + dll)
    else:
        msc['LIBS'].append(pref + v + dll)
        if ld != 'LIBDIR':
            msc['INSTALL'].append((pref + v, pref + v, dll, ld, pref))
        else:
            msc['INSTALL'].append((pref + v, pref + v,
                                   dll, '$(%sdir)' % lib.replace('-', '_'),
                                   pref))

    if libmap.has_key('MTSAFE'):
        fd.write("CFLAGS=$(CFLAGS) $(thread_safe_flag_spec)\n")

    dlib = []
    if libmap.has_key(libname+ "_DLIBS"):
        dlib = libmap[libname+"_DLIBS"]
    liblist = []
    if libmap.has_key("LIBS"):
        liblist = liblist + libmap["LIBS"]
    if libmap.has_key("WINLIBS"):
        liblist = liblist + libmap["WINLIBS"]
    if liblist:
        msc_additional_libs(fd, libname, sep, "LIB", liblist, dlib, msc, pref, dll)

    for src in libmap['SOURCES']:
        base, ext = split_filename(src)
        if ext not in automake_ext:
            msc['EXTRA_DIST'].append(src)

    srcs = pref + sep + libname + "_OBJS ="
    deffile1 = ''
    deffile2 = ''
    for target in libmap['TARGETS']:
        if target == "@LIBOBJS@":
            srcs = srcs + " $(LIBOBJS)"
        else:
            t, ext = split_filename(target)
            if ext == "o":
                srcs = srcs + " " + t + ".$O"
            elif ext == "glue.o":
                srcs = srcs + " " + t + ".glue.$O"
            elif ext == "tab.o":
                srcs = srcs + " " + t + ".tab.$O"
            elif ext == "yy.o":
                srcs = srcs + " " + t + ".yy.$O"
            elif ext == 'res':
                srcs = srcs + " " + t + ".res"
            elif ext in hdrs_ext:
                HDRS.append(target)
            elif ext in scripts_ext:
                if target not in SCRIPTS:
                    SCRIPTS.append(target)
            elif ext == 'def':
                deffile1 = ' "-DEF:$(SRCDIR)\\%s"' % target
                deffile2 = ' --def "$(SRCDIR)\\%s"' % target
    fd.write(srcs + "\n")
    ln = pref + sep + libname
    if libmap.has_key('NOINST'):
        fd.write("%s.lib: $(%s_OBJS)\n" % (ln, ln.replace('-','_')))
        fd.write('\t$(ARCHIVER) $(OUTPUTLIB)"%s.lib" $(%s_OBJS)\n' % (ln, ln.replace('-','_')))
    else:
        fd.write("%s.lib: %s%s\n" % (ln, ln, dll))
        fd.write("!IFDEF USE_MINGW\n")
        fd.write("%s%s: $(%s_OBJS) \n" % (ln, dll, ln.replace('-','_')))
        fd.write("\tdllwrap --driver-name=gcc --mno-cygwin --dllname=%s%s --output-lib=%s.lib $(%s_OBJS) $(LDFLAGS) $(%s_LIBS)%s\n" % (ln, dll, ln, ln.replace('-','_'), ln.replace('-','_'), deffile2))
        fd.write("!ELSE\n")
        fd.write("%s%s: $(%s_OBJS) \n" % (ln, dll, ln.replace('-','_')))
        fd.write("\t$(CC) $(CFLAGS) -LD $(OUTPUTEXE)%s%s $(%s_OBJS) $(LDFLAGS) $(%s_LIBS)%s\n" % (ln, dll, ln.replace('-','_'), ln.replace('-','_'), deffile1))
        fd.write("!ENDIF\n")
    fd.write("\n")

    if SCRIPTS:
        fd.write(libname.replace('-','_')+"_SCRIPTS =" + msc_space_sep_list(SCRIPTS))
        msc['BUILT_SOURCES'].append("$(" + name.replace('-','_') + "_SCRIPTS)")

    if libmap.has_key('HEADERS'):
        for h in HDRS:
            msc['HDRS'].append(h)

    msc_deps(fd, libmap['DEPS'], ".$O", msc)

def msc_libs(fd, var, libsmap, msc):

    lib = "lib"
    ld = "LIBDIR"
    if (libsmap.has_key("DIR")):
        lib = "libs"
        ld = libsmap["DIR"][0] # use first name given
    ld = msc_translate_dir(ld,msc)

    sep = ""
    if libsmap.has_key('SEP'):
        sep = libsmap['SEP'][0]

    SCRIPTS = []
    scripts_ext = []
    if libsmap.has_key('SCRIPTS'):
        scripts_ext = libsmap['SCRIPTS']

    if libsmap.has_key('MTSAFE'):
        fd.write("CFLAGS=$(CFLAGS) $(thread_safe_flag_spec)\n")

    for libsrc in libsmap['SOURCES']:
        libname, ext = split_filename(libsrc)
        if ext not in automake_ext:
            msc['EXTRA_DIST'].append(libsrc)
        v = sep + libname
        msc['LIBS'].append('lib' + v + '.dll')
        msc['INSTALL'].append(('lib' + v, 'lib' + v, '.dll',
                               '$(%sdir)' % lib.replace('-', '_'), ''))

        dlib = []
        if libsmap.has_key(libname + "_DLIBS"):
            dlib = libsmap[libname+"_DLIBS"]
        if libsmap.has_key(libname + "_LIBS"):
            msc_additional_libs(fd, libname, sep, "LIB", libsmap[libname + "_LIBS"], dlib, msc)
        else:
            libslist = []
            if libsmap.has_key("LIBS"):
                libslist = libslist + libsmap["LIBS"]
            if libsmap.has_key("WINLIBS"):
                libslist = libslist + libsmap["WINLIBS"]
            if libslist:
                msc_additional_libs(fd, libname, sep, "LIB", libslist, dlib, msc)

        srcs = "lib"+sep+libname+"_OBJS ="
        deffile1 = ''
        deffile2 = ''
        for target in libsmap['TARGETS']:
            t, ext = split_filename(target)
            if t == libname:
                t, ext = split_filename(target)
                if ext == "o":
                    srcs = srcs + " " + t + ".$O"
                elif ext == "glue.o":
                    srcs = srcs + " " + t + ".glue.$O"
                elif ext == "tab.o":
                    srcs = srcs + " " + t + ".tab.$O"
                elif ext == "yy.o":
                    srcs = srcs + " " + t + ".yy.$O"
                elif ext == 'res':
                    srcs = srcs + " " + t + ".res"
                elif ext in scripts_ext:
                    if target not in SCRIPTS:
                        SCRIPTS.append(target)
                elif ext == 'def':
                    deffile1 = ' "-DEF:$(SRCDIR)\\%s"' % target
                    deffile2 = ' --def "$(SRCDIR)\\%s"' % target
        fd.write(srcs + "\n")
        ln = "lib" + sep + libname
        fd.write(ln + ".lib: " + ln + ".dll\n")
        fd.write("!IFDEF USE_MINGW\n")
        fd.write(ln + ".dll: $(" + ln.replace('-','_') + "_OBJS)\n")
        fd.write("\tdllwrap --driver-name=gcc --mno-cygwin --dllname=%s.dll --output-lib=%s.lib $(%s_OBJS) $(LDFLAGS) $(%s_LIBS)%s\n" % (ln, ln, ln.replace('-','_'), ln.replace('-','_'), deffile2))
        fd.write("!ELSE\n")
        fd.write(ln + ".dll: $(" + ln.replace('-','_') + "_OBJS)\n")
        fd.write("\t$(CC) $(CFLAGS) -LD $(OUTPUTEXE)%s.dll $(%s_OBJS) $(LDFLAGS) $(%s_LIBS)%s\n" % (ln, ln.replace('-','_'), ln.replace('-','_'), deffile1))
        fd.write("!ENDIF\n")
        fd.write("\n")

    if SCRIPTS:
        fd.write("SCRIPTS =" + msc_space_sep_list(SCRIPTS))
        msc['BUILT_SOURCES'].append("$(SCRIPTS)")
        msc['SCRIPTS'].append("$(SCRIPTS)")

    if libsmap.has_key('HEADERS'):
        HDRS = []
        hdrs_ext = libsmap['HEADERS']
        for target in libsmap['DEPS'].keys():
            t, ext = split_filename(target)
            if ext in hdrs_ext:
                msc['HDRS'].append(target)

    if ld != 'LIBDIR':
        fd.write("%sdir = %s\n" % (lib.replace('-','_'), ld))

    msc_deps(fd, libsmap['DEPS'], ".$O", msc)

def msc_includes(fd, var, values, msc):
    incs = "-I$(SRCDIR)"
    for i in values:
        # replace all occurrences of @XXX@ with $(XXX)
        i = re.sub('@([A-Z_]+)@', r'$(\1)', i)
        if i[0] == "-":
            incs = incs + ' "%s"' % i.replace('/', '\\')
        elif i[0] == "$":
            incs = incs + ' %s' % i.replace('/', '\\')
        else:
            incs = incs + ' "-I%s"' % msc_translate_dir(i, msc) \
                   + msc_add_srcdir(i, msc, " -I")
    fd.write("INCLUDES = " + incs + "\n")

def msc_jar(fd, var, jar, msc):

    name = var[4:]

    jd = "JAVADIR"
    if jar.has_key("DIR"):
        jd = jar["DIR"][0] # use first name given
    jd = msc_translate_dir(jd, msc)

    for src in jar['SOURCES']:
        msc['EXTRA_DIST'].append(src)

    fd.write("\n!IFDEF HAVE_JAVA\n\n")

    if jar.has_key("MANIFEST") and len(jar['MANIFEST']) == 1:
        fd.write("%s_manifest_file= %s\n" % (name.replace('-','_'), msc_translate_dir(jar['MANIFEST'][0],msc)))
        manifest_flag='m'
    else:
        fd.write("%s_manifest_file= \n" % name.replace('-','_'))
        manifest_flag=''

    fd.write("%s_java_files= " % (name.replace('-','_')))
    infiles = []
    for j in jar['SOURCES']:
        s,ext = rsplit_filename(j)
        if ext == 'in':
            fd.write('%s ' % msc_basename(msc_translate_dir(s, msc)))
            infiles.append(msc_translate_dir(s, msc))
        else:
            fd.write('$(SRCDIR)\\%s ' % msc_translate_dir(j,msc))
    fd.write('\n')
    for infile in infiles:
        fd.write('%s: "$(SRCDIR)\\%s.in"\n' % (msc_basename(infile), infile))
        fd.write('\t$(CONFIGURE) "$(SRCDIR)\\%s.in" > "%s"\n' % (infile, msc_basename(infile)))

    fd.write("\n%s_class_files= " % (name.replace('-','_')))
    for j in jar['TARGETS']:
        fd.write('"%s" ' % msc_translate_dir(j,msc))

    fd.write("\n$(%s_class_files): $(%s_java_files)\n" % (name.replace('-','_'), name.replace('-','_')))
    fd.write("\t$(JAVAC) -d . -classpath \"$(CLASSPATH)\" $(JAVACFLAGS) $(%s_java_files)\n" % name.replace('-','_'))

    fd.write("%s.jar: $(%s_class_files) $(%s_manifest_file)\n" % (name, name.replace('-','_'), name.replace('-','_')))
    fd.write("\t$(JAR) $(JARFLAGS) -cf%s $@ $(%s_manifest_file) $(%s_class_files)\n" % (manifest_flag, name.replace('-','_'), name.replace('-','_')))

    fd.write('install_%s: %s.jar\n' % (name, name))
    fd.write('\tif not exist "%s" $(MKDIR) "%s"\n' % (jd, jd))
    fd.write('\t$(INSTALL) %s.jar "%s\\%s.jar"\n' % (name, jd, name))

    fd.write('%s: %s.jar\n' % (name, name))

    fd.write("\n!ELSE\n\n")

    fd.write('%s:\n' % name)
    fd.write('install_%s:\n' % name)

    fd.write("\n!ENDIF #HAVE_JAVA\n\n")

    msc['SCRIPTS'].append(name)
    msc['INSTALL'].append((name, name, '', None, ''))

def msc_java(fd, var, java, msc):

    name = var[5:]

    jd = "JAVADIR"
    if java.has_key("DIR"):
        jd = java["DIR"][0] # use first name given
    jd = msc_translate_dir(jd, msc)

    for src in java['SOURCES']:
        msc['EXTRA_DIST'].append(src)

    fd.write("\n!IFDEF HAVE_JAVA\n\n")

    fd.write("%s_java_files= " % (name.replace('-','_')))
    for j in java['SOURCES']:
        s,ext = rsplit_filename(j)
        if ext == 'in':
            fd.write('%s ' % s)
        else:
            fd.write('$(SRCDIR)\\%s ' % msc_translate_dir(j,msc))

    fd.write("\n%s_class_files= " % (name.replace('-','_')))
    for j in java['TARGETS']:
        fd.write('"%s" ' % msc_translate_dir(j,msc))

    fd.write("\n$(%s_class_files): $(%s_java_files)\n" % (name.replace('-','_'), name.replace('-','_')))
    fd.write("\t$(JAVAC) -d . -classpath \"$(CLASSPATH)\" $(JAVACFLAGS) $(%s_java_files)\n" % name.replace('-','_'))

    fd.write('install_%s: $(%s_class_files)\n' % (name, name.replace('-','_')))
    fd.write('\tif not exist "%s" $(MKDIR) "%s"\n' % (jd, jd))
    fd.write('\t$(INSTALL) $(%s_class_files) "%s\\$(%s_class_files)"\n' % (name.replace('-','_'), jd, name.replace('-','_')))

    fd.write('%s: $(%s_class_files)\n' % (name, name.replace('-','_')))

    fd.write("\n!ELSE\n\n")

    fd.write('%s:\n' % name)
    fd.write('install_%s:\n' % name)

    fd.write("\n!ENDIF #HAVE_JAVA\n\n")

    msc['SCRIPTS'].append(name)
    msc['INSTALL'].append((name, name, '', None, ''))

output_funcs = {'SUBDIRS': msc_subdirs,
                'EXTRA_DIST': msc_extra_dist,
                'EXTRA_HEADERS': msc_extra_headers,
                'LIBDIR': msc_libdir,
                'LIBS': msc_libs,
                'LIB': msc_library,
                'BINS': msc_bins,
                'BIN': msc_binary,
                'DOC': msc_doc,
                'SCRIPTS': msc_scripts,
                'INCLUDES': msc_includes,
                'MTSAFE': msc_mtsafe,
                'CFLAGS': msc_cflags,
                'CXXFLAGS': msc_cflags,
                'STATIC_MODS': msc_mods_to_libs,
                'smallTOC_SHARED_MODS': msc_mods_to_libs,
                'largeTOC_SHARED_MODS': msc_mods_to_libs,
                'HEADERS': msc_headers,
                'JAR': msc_jar,
                'JAVA': msc_java,
                }

def output(tree, cwd, topdir):
    # HACKS to keep uncompilable stuff out of Windows makefiles.
    if tree.has_key('bin_Mtimeout'):
        tree = tree.copy()
        del tree['bin_Mtimeout']
    if tree.has_key('LIB_mprof'):
        tree = tree.copy()
        del tree['LIB_mprof']

    fd = open(os.path.join(cwd, 'Makefile.msc'), "w")

    fd.write(MAKEFILE_HEAD)

    if not tree.has_key('INCLUDES'):
        tree.add('INCLUDES', [])

    msc = {}
    msc['BUILT_SOURCES'] = []
    msc['EXTRA_DIST'] = []
    msc['LIBS'] = []            # libraries which are installed (DLLs)
    msc['NLIBS'] = []           # archives (libraries which are not installed)
    msc['SCRIPTS'] = []
    msc['BINS'] = []
    msc['HDRS'] = []
    msc['INSTALL'] = []
    msc['LIBDIR'] = 'lib'
    msc['TREE'] = tree
    msc['cwd'] = cwd
    msc['DEPS'] = []
    msc['_IN'] = []

    prefix = os.path.commonprefix([cwd, topdir])
    d = cwd[len(prefix):]
    reldir = os.curdir
    srcdir = d
    if len(d) > 1 and d[0] == os.sep:
        d = d[len(os.sep):]
        while d:
            reldir = os.path.join(reldir, os.pardir)
            d, t = os.path.split(d)

    fd.write("TOPDIR = %s\n" % string.replace(reldir, '/', '\\'))
    fd.write("SRCDIR = $(TOPDIR)\\..%s\n" % string.replace(srcdir, '/', '\\'))
    fd.write("!INCLUDE $(TOPDIR)\\rules.msc\n")
    if tree.has_key("SUBDIRS"):
        fd.write("all: all-recursive all-msc\n")
        fd.write("check: check-recursive check-msc\n")
        fd.write("install: install-recursive install-msc\n")
    else:
        fd.write("all: all-msc\n")
        fd.write("check: check-msc\n")
        fd.write("install: install-msc\n")

    for i, v in tree.items():
        j = i
        if string.find(i, '_') >= 0:
            k, j = string.split(i, '_', 1)
            j = string.upper(k)
        if type(v) is type([]):
            if output_funcs.has_key(i):
                output_funcs[i](fd, i, v, msc)
            elif output_funcs.has_key(j):
                output_funcs[j](fd, i, v, msc)
            elif i != 'TARGETS':
                msc_assignment(fd, i, v, msc)

    for i, v in tree.items():
        j = i
        if string.find(i, '_') >= 0:
            k, j = string.split(i, '_', 1)
            j = string.upper(k)
        if type(v) is type({}):
            if output_funcs.has_key(i):
                output_funcs[i](fd, i, v, msc)
            elif output_funcs.has_key(j):
                output_funcs[j](fd, i, v, msc)
            elif i != 'TARGETS':
                msc_assignment(fd, i, v, msc)

    if msc['BUILT_SOURCES']:
        fd.write("BUILT_SOURCES = ")
        for v in msc['BUILT_SOURCES']:
            fd.write(" %s" % v)
        fd.write("\n")

##    fd.write("EXTRA_DIST = Makefile.ag Makefile.msc")
##    for v in msc['EXTRA_DIST']:
##        fd.write(" %s" % v)
##    fd.write("\n")

    fd.write("all-msc:")
    if msc['LIBS']:
        for v in msc['LIBS']:
            fd.write(' "%s"' % v)

    if msc['NLIBS']:
        for v in msc['NLIBS']:
            fd.write(' "%s"' % v)

    if msc['BINS']:
        for v in msc['BINS']:
            fd.write(' "%s.exe"' % v)

    if msc['SCRIPTS']:
        for v in msc['SCRIPTS']:
            fd.write(' "%s"' % v)

    fd.write("\n")

    fd.write("check-msc: all-msc")
    if msc['INSTALL']:
        for (dst, src, ext, dir, pref) in msc['INSTALL']:
            fd.write(' "%s%s"' % (src, ext))
    fd.write("\n")

    fd.write("install-msc: install-exec install-data\n")
    l = []
    for (dst, src, ext, dir, pref) in msc['INSTALL']:
        l.append(dst)

    fd.write("install-exec: %s %s\n" % (
        msc_list2string(msc['BINS'], '"install_bin_','" '),
        msc_list2string(l, '"install_','" ')))
    if msc['BINS']:
        for v in msc['BINS']:
            fd.write('install_bin_%s: "%s.exe"\n' % (v, v))
            fd.write('\tif not exist "$(%sdir)" $(MKDIR) "$(%sdir)"\n' % (v.replace('-','_'), v.replace('-','_')))
            fd.write('\t$(INSTALL) "%s.exe" "$(%sdir)"\n' % (v,v.replace('-','_')))
    if msc['INSTALL']:
        for (dst, src, ext, dir, pref) in msc['INSTALL']:
            if not dir:
                continue
            fd.write('install_%s: "%s%s" "%s"\n' % (dst, src, ext, dir))
            fd.write('\t$(INSTALL) "%s%s" "%s\\%s%s"\n' % (src, ext, dir, dst, ext))
            if pref:
                fd.write('\t$(INSTALL) "%s%s" "%s\\%s%s"\n' % (src, '.lib', dir, dst, '.lib'))
        td = {}
        for (x, y, u, dir, pref) in msc['INSTALL']:
            if not dir:
                continue
            if not td.has_key(dir):
                fd.write('"%s":\n' % dir)
                fd.write('\tif not exist "%s" $(MKDIR) "%s"\n' % (dir, dir))
                td[dir] = 1

    fd.write("install-data:")
    if cwd != topdir and msc['HDRS']:
        name=os.path.split(cwd)[1]
        if name:
            tHDRS = []
            for h in msc['HDRS']:
                tHDRS.append(msc_translate_dir(msc_translate_file(h, msc), msc))
            fd.write(" install-%sinclude_HEADERS\n" % name)
            fd.write("%sincludedir = $(includedir)\\%s\n" % (name.replace('-','_'), name))
            fd.write("%sinclude_HEADERS = %s\n" % (name.replace('-','_'), msc_list2string(tHDRS, " ", "")))
            fd.write("install-%sinclude_HEADERS: $(%sinclude_HEADERS)\n" % (name,name.replace('-','_')))
            fd.write('\tif not exist "$(%sincludedir)" $(MKDIR) "$(%sincludedir)"\n' % (name.replace('-','_'),name.replace('-','_')))
            for h in tHDRS:
                fd.write('\t$(INSTALL) "%s" "$(%sincludedir)"\n' % (h,name.replace('-','_')))
    fd.write("\n")
