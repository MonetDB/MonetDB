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
# Portions created by CWI are Copyright (C) 1997-2003 CWI.
# All Rights Reserved.
# 
# Contributor(s):
# 		Martin Kersten <Martin.Kersten@cwi.nl>
# 		Peter Boncz <Peter.Boncz@cwi.nl>
# 		Niels Nes <Niels.Nes@cwi.nl>
# 		Stefan Manegold  <Stefan.Manegold@cwi.nl>

import string
import os

#automake_ext = ['c', 'cc', 'h', 'y', 'yy', 'l', 'll', 'glue.c']
automake_ext = ['c', 'cc', 'h', 'tab.c', 'tab.cc', 'tab.h', 'yy.c', 'yy.cc', 'glue.c', 'proto.h', '']
script_ext = ['mil']

def split_filename(f):
    base = f
    ext = ""
    if string.find(f, ".") >= 0:
        return string.split(f, ".", 1)
    return base, ext

def msc_dummy(fd, var, values, msc):
    res = fd

def msc_list2string(l, pre, post):
    res = ""
    for i in l:
        res = res + pre + i + post
    return res

def msc_subdirs(fd, var, values, msc):
    # HACK to cope with conditional subdirs for cross-compiling:
    Vals = []
    for v in values:
        Vals.append(string.replace(v,'CROSS_COMPILING?:',''))
    values = Vals
    # HACK to keep uncompilable stuff out of Windows makefiles.
    if 'calibrator' in values:
        values = values[:]
        values.remove('calibrator')
    fd.write("%s = %s\n" % (var, string.join(values)))
    fd.write("all-recursive: %s\n" % msc_list2string(values, "", "-all "))
    for v in values:
        fd.write("%s-all: %s %s\\Makefile\n" % (v, v, v))
        fd.write("\t$(CD) %s && $(MAKE) /nologo all \n" % v)
        fd.write("%s: \n\tif not exist %s $(MKDIR) %s\n" % (v, v, v))
        fd.write("%s\\Makefile: $(SRCDIR)\\%s\\Makefile.msc\n" % (v, v))
        fd.write("\t$(INSTALL) $(SRCDIR)\\%s\\Makefile.msc %s\\Makefile\n" % (v, v))
    fd.write("check-recursive: %s\n" % msc_list2string(values, "", "-check "))
    for v in values:
        fd.write("%s-check: %s\n" % (v, v))
        fd.write("\t$(CD) %s && $(MAKE) /nologo check\n" % v)
    fd.write("install-recursive: %s\n" % msc_list2string(values, "", "-install "))
    for v in values:
        fd.write("%s-install: $(bindir) $(libdir)\n" % v)
        fd.write("\t$(CD) %s && $(MAKE) /nologo install\n" % v)

def msc_assignment(fd, var, values, msc):
    o = ""
    for v in values:
        o = o + " " + string.replace(v, '/', '\\')
    fd.write("%s = %s\n" % (var, o))

def msc_cflags(fd, var, values, msc):
    o = ""
    for v in values:
        o = o + " " + v
    fd.write("%s = $(%s) %s\n" % (var, var, o))

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
    elif dir in ['bindir', 'sbindir', 'libexecdir', 'datadir', 'sysconfdir',
                 'sharedstatedir', 'localstatedir', 'libdir', 'infodir',
                 'mandir', 'includedir', 'oldincludedir', 'pkgdatadir',
                 'pkglibdir', 'pkgincludedir']:
        dir = "$("+dir+")"
    if len(rest) > 0:
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

def msc_additional_libs(fd, name, sep, type, list, dlibs, msc):
    deps = "lib"+sep+name+".dll: "
    if type == "BIN":
        add = name+"_LIBS ="
    elif type == "LIB":
        add = "lib"+sep+name+"_LIBS ="
    else:
        add = name + " ="
    for l in list:
        if l == "@LIBOBJS@":
            add = add + " $(LIBOBJS)"
        elif l[0] in  ("-", "$"):
            add = add + " " + l
        elif l[0] not in  ("@"):
            add = add + " " + msc_translate_dir(l, msc) + ".lib"
            deps = deps + " " + msc_translate_dir(l, msc) + ".lib"
    for l in dlibs:
        if l == "@LIBOBJS@":
            add = add + " $(LIBOBJS)"
        elif l[0] in  ("-", "$"):
            add = add + " " + l
        elif l[0] not in  ("@"):
            add = add + " " + msc_translate_dir(l, msc) + ".lib"
            deps = deps + " " + msc_translate_dir(l, msc) + ".lib"
    if type != "MOD":
        fd.write(deps + "\n")
    return add + "\n"

def msc_translate_ext(f):
    n = string.replace(f, '.o', '.obj')
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
    if len(msc['DEPS']) <= 0:
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
                        fd.write(sep + dep)
                        sep = " "
                else:
                    print "!WARNING: dropped absolute dependency " + d
            if sep == " ":
                fd.write("\n")
                if tf+'.mx.in' in deplist:
                    fd.write('\t$(MX) $(MXFLAGS) -x sh %s.mx\n' % tf)
            for x, y in _in:
                # TODO
                # replace this hack by something like configure ...
                fd.write("%s: %s\n" % (x, y))
                fd.write("\tif exist %s $(CONFIGURE) %s > %s\n" % (y, y, x))
                msc['_IN'].append(y)
            if ext == "tab.h":
                x, de = split_filename(deplist[0])
                if de == 'y':
                    fd.write("\t$(YACC) $(YFLAGS) %s.y\n" % b)
                    fd.write("\tif exist y.tab.c $(DEL) y.tab.c\n")
                    fd.write("\tif exist y.tab.h $(MV) y.tab.h %s.tab.h\n" % b)
                else:
                    fd.write("\t$(YACC) $(YFLAGS) %s.yy\n" % b)
                    fd.write("\tif exist y.tab.c $(DEL) y.tab.c\n")
                    fd.write("\tif exist y.tab.h $(MV) y.tab.h %s.tab.h\n" % b)
            if ext == "tab.c":
                fd.write("\t$(YACC) $(YFLAGS) %s.y\n" % b)
                fd.write("\tif exist y.tab.c $(MV) y.tab.c %s.tab.c\n" % b)
                fd.write("\tif exist y.tab.h $(DEL) y.tab.h\n")
            if ext == "tab.cxx":
                fd.write("\t$(YACC) $(YFLAGS) %s.yy\n" % b)
                fd.write("\tif exist y.tab.c $(MV) y.tab.c %s.tab.cxx\n" % b)
                fd.write("\tif exist y.tab.h $(DEL) y.tab.h\n")
            if ext == "yy.c":
                fd.write("\t$(LEX) $(LFLAGS) %s.l\n" % b)
                fd.write("\tif exist lex.yy.c $(MV) lex.yy.c %s.yy.c\n" % b)
            if ext == "yy.cxx":
                fd.write("\t$(LEX) $(LFLAGS) %s.ll\n" % b)
                fd.write("\tif exist lex.yy.c $(MV) lex.yy.c %s.yy.cxx\n" % b)

            if ext == "glue.c":
                fd.write("\t$(MEL) $(INCLUDES) -o %s -glue %s.m\n" % (t, b))
            if ext == "proto.h":
                fd.write("\t$(MEL) $(INCLUDES) -o %s -proto %s.m\n" % (t, b))
            if ext in ("obj", "glue.obj", "tab.obj", "yy.obj"):
                target, name = msc_find_target(tar, msc)
                if name[0] == '_':
                    name = name[1:]
                if target == "LIB":
                    d, dext = split_filename(deplist[0])
                    if dext == "c" or dext == "glue.c":
                        fd.write("\t$(CC) $(CFLAGS) $(INCLUDES) -DLIB%s -c %s\n" %
                                 (name, msc_translate_ext(deplist[0])))
                    elif dext == "cc":
                        fd.write("\t$(CXX) $(CXXFLAGS) $(INCLUDES) -DLIB%s -c %s\n" %
                                 (name, msc_translate_ext(deplist[0])))
    msc['DEPS'].append("DONE")

# list of scripts to install
def msc_scripts(fd, var, scripts, msc):

    sd = "SCRIPTSDIR"
    if scripts.has_key("DIR"):
        sd = scripts["DIR"][0] # use first name given
    sd = msc_translate_dir(sd, msc)

    for script in scripts['TARGETS']:
        if os.path.isfile(os.path.join(msc['cwd'], script+'.in')):
            inf = '$(SRCDIR)\\%s.in' % script
            if inf not in msc['_IN']:
                # TODO
                # replace this hack by something like configure ...
                fd.write("%s: %s\n" % (script, inf))
                fd.write("\tif exist %s $(CONFIGURE) %s > %s\n" % (inf, inf, script))
                msc['_IN'].append(inf)
        elif os.path.isfile(os.path.join(msc['cwd'], script)):
            fd.write("%s: $(SRCDIR)\\%s\n" % (script, script))
            fd.write("\tif exist $(SRCDIR)\\%s $(INSTALL) $(SRCDIR)\\%s %s\n" % (script, script, script))
        msc['INSTALL'].append((script, script, '', sd))

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
            fd.write("%s: $(SRCDIR)\\%s\n" % (header, header))
##            fd.write("\t$(INSTALL) $(SRCDIR)\\%s %s\n" % (header, header))
##            fd.write("\tif not exist %s if exist $(SRCDIR)\\%s $(INSTALL) $(SRCDIR)\\%s %s\n" % (header, header, header, header))
            fd.write("\tif exist $(SRCDIR)\\%s $(INSTALL) $(SRCDIR)\\%s %s\n" % (header, header, header))
            msc['INSTALL'].append((header, header, '', sd))

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
                    fd.write("%s: $(SRCDIR)\\%s.in\n" % (i, i))
                    fd.write("\tif exist $(SRCDIR)\\%s.in $(CONFIGURE) $(SRCDIR)\\%s.in > %s\n" % (i, i, i))
                else:
                    fd.write("%s: $(SRCDIR)\\%s\n" % (i, i))
                    fd.write("\tif exist $(SRCDIR)\\%s $(INSTALL) $(SRCDIR)\\%s %s\n" % (i, i, i))
                msc['INSTALL'].append((i, i, '', '$(bindir)'))
        else: # link
            src = binmap[0][4:]
            n_nme, n_ext = split_filename(name)
            s_nme, s_ext = split_filename(src)
            if n_ext  and  s_ext  and  n_ext == s_ext:
                ext = ''
            else:
                ext = '.exe'
            msc['INSTALL'].append((name, src, ext, '$(bindir)'))
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
    msc['BINS'].append(binname)
    if binmap.has_key('MTSAFE'):
        fd.write("CFLAGS=$(CFLAGS) $(thread_safe_flag_spec)\n")

    if binmap.has_key("LIBS"):
        fd.write(msc_additional_libs(fd, binname, "", "BIN", binmap["LIBS"], [], msc))

    srcs = binname+"_OBJS ="
    for target in binmap['TARGETS']:
        t, ext = split_filename(target)
        if ext == "o":
            srcs = srcs + " " + t + ".obj"
        elif ext == "glue.o":
            srcs = srcs + " " + t + ".glue.obj"
        elif ext == "tab.o":
            srcs = srcs + " " + t + ".tab.obj"
        elif ext == "yy.o":
            srcs = srcs + " " + t + ".yy.obj"
        elif ext in hdrs_ext:
            HDRS.append(target)
        elif ext in scripts_ext:
            if target not in SCRIPTS:
                SCRIPTS.append(target)
    fd.write(srcs + "\n")
    fd.write("%s.exe: $(%s_OBJS)\n" % (binname, binname))
    fd.write("\t$(CC) $(CFLAGS) -Fe%s.exe $(%s_OBJS) $(%s_LIBS) $(LDFLAGS) /subsystem:console /NODEFAULTLIB:LIBC\n\n" % (binname, binname, binname))

    if len(SCRIPTS) > 0:
        fd.write(binname+"_SCRIPTS =" + msc_space_sep_list(SCRIPTS))
        msc['BUILT_SOURCES'].append("$(" + binname + "_SCRIPTS)")

    if binmap.has_key('HEADERS'):
        for h in HDRS:
            msc['HDRS'].append(h)

    msc_deps(fd, binmap['DEPS'], ".obj", msc)

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
        msc['BINS'].append(bin)

        if binsmap.has_key(bin + "_LIBS"):
            fd.write(msc_additional_libs(fd, bin, "", "BIN", binsmap[bin + "_LIBS"], [], msc))
        elif binsmap.has_key("LIBS"):
            fd.write(msc_additional_libs(fd, bin, "", "BIN", binsmap["LIBS"], [], msc))

        srcs = bin+"_OBJS ="
        for target in binsmap['TARGETS']:
            t, ext = split_filename(target)
            if t == bin:
                t, ext = split_filename(target)
                if ext == "o":
                    srcs = srcs + " " + t + ".obj"
                elif ext == "glue.o":
                    srcs = srcs + " " + t + ".glue.obj"
                elif ext == "tab.o":
                    srcs = srcs + " " + t + ".tab.obj"
                elif ext == "yy.o":
                    srcs = srcs + " " + t + ".yy.obj"
                elif ext in hdrs_ext:
                    HDRS.append(target)
                elif ext in scripts_ext:
                    if target not in SCRIPTS:
                        SCRIPTS.append(target)
        fd.write(srcs + "\n")
        fd.write("%s.exe: $(%s_OBJS)\n" % (bin, bin))
        fd.write("\t$(CC) $(CFLAGS) -Fe%s.exe $(%s_OBJS) $(%s_LIBS) $(LDFLAGS) /subsystem:console /NODEFAULTLIB:LIBC\n\n" % (bin, bin, bin))

    if len(SCRIPTS) > 0:
        fd.write(name+"_SCRIPTS =" + msc_space_sep_list(SCRIPTS))
        msc['BUILT_SOURCES'].append("$(" + name + "_SCRIPTS)")

    if binsmap.has_key('HEADERS'):
        for h in HDRS:
            msc['HDRS'].append(h)

    msc_deps(fd, binsmap['DEPS'], ".obj", msc)

def msc_mods_to_libs(fd, var, modmap, msc):
    modname = var[:-4]+"LIBS"
    msc_assignment(fd, var, modmap, msc)
    fd.write(msc_additional_libs(fd, modname, "", "MOD", modmap, [], msc))

def msc_library(fd, var, libmap, msc):

    name = var[4:]
    sep = ""
    if (libmap.has_key("NAME")):
        libname = libmap['NAME'][0]
    else:
        libname = name

    if (libname[0] == "_"):
        sep = "_"
        libname = libname[1:]

    lib = "lib"
    ld = "LIBDIR"
    if (libmap.has_key("DIR")):
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
    msc['LIBS'].append(v)
    msc['INSTALL'].append(('lib'+v,'lib'+v,'.dll','$('+lib+'dir)'))
    if ld != 'LIBDIR':
        fd.write("%sdir = %s\n" % (lib,ld))

    if libmap.has_key('MTSAFE'):
        fd.write("CFLAGS=$(CFLAGS) $(thread_safe_flag_spec)\n")

    dlib = []
    if libmap.has_key(libname+ "_DLIBS"):
        dlib = libmap[libname+"_DLIBS"]
    liblist = []
    if libmap.has_key("WINLIBS"):
        liblist += libmap["WINLIBS"]
    if libmap.has_key("LIBS"):
        liblist += libmap["LIBS"]
    if liblist:
        fd.write(msc_additional_libs(fd, libname, sep, "LIB", liblist, dlib, msc))

    for src in libmap['SOURCES']:
        base, ext = split_filename(src)
        if ext not in automake_ext:
            msc['EXTRA_DIST'].append(src)

    srcs = "lib" + sep + libname + "_OBJS ="
    for target in libmap['TARGETS']:
        if target == "@LIBOBJS@":
            srcs = srcs + " $(LIBOBJS)"
        else:
            t, ext = split_filename(target)
            if ext == "o":
                    srcs = srcs + " " + t + ".obj"
            elif ext == "glue.o":
                    srcs = srcs + " " + t + ".glue.obj"
            elif ext == "tab.o":
                    srcs = srcs + " " + t + ".tab.obj"
            elif ext == "yy.o":
                    srcs = srcs + " " + t + ".yy.obj"
            elif ext in hdrs_ext:
                    HDRS.append(target)
            elif ext in scripts_ext:
                    if target not in SCRIPTS:
                            SCRIPTS.append(target)
    fd.write(srcs + "\n")
    ln = "lib" + sep + libname
    fd.write(ln + ".lib: " + ln + ".dll\n")
    fd.write(ln + ".dll: $(" + ln + "_OBJS) \n")
    fd.write("\t$(CC) $(CFLAGS) -LD -Fe%s.dll $(%s_OBJS) $(%s_LIBS) $(LDFLAGS)\n\n" % (ln, ln, ln))

    if len(SCRIPTS) > 0:
        fd.write(libname+"_SCRIPTS =" + msc_space_sep_list(SCRIPTS))
        msc['BUILT_SOURCES'].append("$(" + name + "_SCRIPTS)")

    if libmap.has_key('HEADERS'):
        for h in HDRS:
            msc['HDRS'].append(h)

    msc_deps(fd, libmap['DEPS'], ".obj", msc)

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
        msc['LIBS'].append(v)
        msc['INSTALL'].append(('lib'+v,'lib'+v,'.dll','$('+lib+'dir)'))

        dlib = []
        if libsmap.has_key(libname + "_DLIBS"):
            dlib = libsmap[libname+"_DLIBS"]
        if libsmap.has_key(libname + "_LIBS"):
            fd.write(msc_additional_libs(fd, libname, sep, "LIB", libsmap[libname + "_LIBS"], dlib, msc))
        else:
            libslist = []
            if libsmap.has_key("WINLIBS"):
                libslist += libsmap["WINLIBS"]
            if libsmap.has_key("LIBS"):
                libslist += libsmap["LIBS"]
            if libslist:
                fd.write(msc_additional_libs(fd, libname, sep, "LIB", libslist, dlib, msc))

        srcs = "lib"+sep+libname+"_OBJS ="
        for target in libsmap['TARGETS']:
            t, ext = split_filename(target)
            if t == libname:
                t, ext = split_filename(target)
                if ext == "o":
                    srcs = srcs + " " + t + ".obj"
                elif ext == "glue.o":
                    srcs = srcs + " " + t + ".glue.obj"
                elif ext == "tab.o":
                    srcs = srcs + " " + t + ".tab.obj"
                elif ext == "yy.o":
                    srcs = srcs + " " + t + ".yy.obj"
                elif ext in scripts_ext:
                    if target not in SCRIPTS:
                        SCRIPTS.append(target)
        fd.write(srcs + "\n")
        ln = "lib" + sep + libname
        fd.write(ln + ".lib: " + ln + ".dll\n")
        fd.write(ln + ".dll: $(" + ln + "_OBJS)\n")
        fd.write("\t$(CC) $(CFLAGS) -LD -Fe%s.dll $(%s_OBJS) $(%s_LIBS) $(LDFLAGS)\n\n" % (ln, ln, ln))

    if len(SCRIPTS) > 0:
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
        fd.write("%sdir = %s\n" % (lib, ld))

    msc_deps(fd, libsmap['DEPS'], ".obj", msc)

def msc_includes(fd, var, values, msc):
    incs = "-I$(SRCDIR)"
    for i in values:
        if i[0] == "-" or i[0] == "$":
            incs = incs + " " + string.replace(i, '/', '\\')
        else:
            incs = incs + " -I" + msc_translate_dir(i, msc) \
                   + msc_add_srcdir(i, msc, " -I")
    fd.write("INCLUDES = " + incs + "\n")

def msc_jar(fd, var, values, msc):
    print "msc doesn't support jars (yet)!"

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
                }

def output(tree, cwd, topdir):
    # HACKS to keep uncompilable stuff out of Windows makefiles.
    if tree.has_key('bin_Mtimeout'):
        tree = tree.copy()
        del tree['bin_Mtimeout']
    if tree.has_key('LIBS') and tree['LIBS'].has_key('SOURCES') and 'mprof.mx' in tree['LIBS']['SOURCES']:
        tree = tree.copy()
        tree['LIBS'] = tree['LIBS'].copy()
        tree['LIBS']['SOURCES'] = tree['LIBS']['SOURCES'][:]
        tree['LIBS']['SOURCES'].remove('mprof.mx')
        targets = tree['LIBS']['TARGETS']
        tree['LIBS']['TARGETS'] = []
        for t in targets:
            if t[:6] != 'mprof.':
                tree['LIBS']['TARGETS'].append(t)

    fd = open(os.path.join(cwd, 'Makefile.msc'), "w")

    fd.write('''
## Use: nmake -f makefile.msc install

# Change this to wherever you want to install the DLLs. This directory
# should be in your PATH.
BIN = C:\\bin

################################################################

# Nothing much configurable below

# cl -? describes the options
CC = cl -GF -W3 -MD -nologo -Zi -G6
# optimize use -Ox

# No general LDFLAGS needed
LDFLAGS = /link
INSTALL = copy
# TODO
# replace this hack by something like configure ...
MKDIR = mkdir
ECHO = echo
CD = cd

CFLAGS = -I. -I$(TOPDIR) $(LIBC_INCS) -DHAVE_CONFIG_H $(INCLUDES)
CXXFLAGS = $(CFLAGS)

CXXEXT = \\\"cxx\\\"

''')

    if not tree.has_key('INCLUDES'):
        tree.add('INCLUDES', [])

    msc = {}
    msc['BUILT_SOURCES'] = []
    msc['EXTRA_DIST'] = []
    msc['LIBS'] = []
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
        while len(d) > 0:
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

    if len(msc['BUILT_SOURCES']) > 0:
        fd.write("BUILT_SOURCES = ")
        for v in msc['BUILT_SOURCES']:
            fd.write(" %s" % v)
        fd.write("\n")

##    fd.write("EXTRA_DIST = Makefile.ag Makefile.msc")
##    for v in msc['EXTRA_DIST']:
##        fd.write(" %s" % v)
##    fd.write("\n")

    fd.write("all-msc:")
    if len(msc['LIBS']) > 0:
        for v in msc['LIBS']:
            fd.write(" lib%s.dll" % v)

    if len(msc['BINS']) > 0:
        for v in msc['BINS']:
            fd.write(" %s.exe" % v)

    if len(msc['SCRIPTS']) > 0:
        for v in msc['SCRIPTS']:
            fd.write(" %s" % v)

    fd.write("\n")

    fd.write("check-msc: all-msc")
    if len(msc['INSTALL']) > 0:
        for (dst, src, ext, dir) in msc['INSTALL']:
            fd.write(" %s%s" % (src, ext))
    fd.write("\n")

    fd.write("install-msc: install-exec install-data\n")
    l = []
    for (x, y, u, v) in msc['INSTALL']:
        l.append(x)
                  #msc_list2string(msc['LIBS'], "install_dll_"," "), \

##    fd.write("install-exec: %s %s %s\n" % (

    fd.write("install-exec: %s %s\n" % (
        msc_list2string(msc['BINS'], "install_bin_"," "),
        msc_list2string(l, "install_"," ")
        ))
##    if len(msc['LIBS']) > 0:
##        for v in msc['LIBS']:
##            fd.write("install_dll_%s: lib%s.dll\n" % (v, v))
##            fd.write("\t$(INSTALL) lib%s.dll $(%sdir)\n" % (v, msc['LIBDIR']))
    if len(msc['BINS']) > 0:
        for v in msc['BINS']:
            fd.write("install_bin_%s: %s.exe\n" % (v, v))
            fd.write("\t$(INSTALL) %s.exe $(bindir)\n" % v)
    if len(msc['INSTALL']) > 0:
        for (dst, src, ext, dir) in msc['INSTALL']:
            fd.write("install_%s: %s%s %s\n" % (dst, src, ext, dir))
            fd.write("\t$(INSTALL) %s%s %s\\%s%s\n" % (src, ext, dir, dst, ext))
        td = []
        for (x, y, u, dir) in msc['INSTALL']:
            if td.count(dir) == 0:
                fd.write("%s:\n" % dir)
                fd.write("\tif not exist %s $(MKDIR) %s\n" % (dir, dir))
                td.append(dir)

    fd.write("install-data:")
##    if len(msc['HDRS']) > 0:
##        if len(name) > 0:
##            fd.write(" install-%s" % v)
##            for v in msc['HDRS']:
##                fd.write("%sincludedir = $(includedir)/%s\n" % (name, name))
##        fd.write("%sinclude_HEADERS = %s\n" % (name, msc_list2string(am['HDRS'], " ", "")))
    fd.write("\n")
