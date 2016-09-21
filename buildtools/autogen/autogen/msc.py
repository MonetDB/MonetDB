# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

import string
import os
import re
from filesplit import rsplit_filename, split_filename, automake_ext

# the text that is put at the top of every generated Makefile.msc
MAKEFILE_HEAD = '''
## Use: nmake -f makefile.msc install

# Nothing much configurable below

'''

def msc_basename(f):
    # return basename (i.e. just the file name part) of a path, no
    # matter which directory separator was used
    return f.split('/')[-1].split('\\')[-1]

def msc_dummy(fd, var, values, msc):
    res = fd

def msc_list2string(l, pre, post):
    res = ""
    for i in l:
        res = res + pre + i + post
    return res

def create_dir(fd, v, n, i):
    # Stupid Windows/nmake cannot cope with single-letter directory
    # names; apparently, it treats them as drive-letters, unless we
    # explicitely call them ".\?".
    if len(v) == 1:
        vv = '.\\%s' % v
    else:
        vv = v
    fd.write('%s-%d-all: "%s-%d-dir" "%s-%d-Makefile"\n' % (n, i, n, i, n, i))
    fd.write('\t$(CD) "%s" && $(MAKE) /nologo $(MAKEDEBUG) "prefix=$(prefix)" "bits=$(bits)" all \n' % vv)
    fd.write('%s-%d-dir: \n\tif not exist "%s" $(MKDIR) "%s"\n' % (n, i, vv, vv))
    fd.write('%s-%d-Makefile: "$(srcdir)\\%s\\Makefile.msc"\n' % (n, i, v))
    fd.write('\t$(INSTALL) "$(srcdir)\\%s\\Makefile.msc" "%s\\Makefile"\n' % (v, v))
    fd.write('%s-%d-check:\n' % (n, i))
    fd.write('\t$(CD) "%s" && $(MAKE) /nologo $(MAKEDEBUG) "prefix=$(prefix)" "bits=$(bits)" check\n' % vv)

    fd.write('%s-%d-install:\n' % (n, i))
    fd.write('\t$(CD) "%s" && $(MAKE) /nologo $(MAKEDEBUG) "prefix=$(prefix)" "bits=$(bits)" install\n' % vv)

def empty_dir(fd, n, i):
    fd.write('%s-%d-all:\n' % (n, i))
    fd.write('%s-%d-check:\n' % (n, i))
    fd.write('%s-%d-install:\n' % (n, i))

def create_subdir(fd, dir, i):
    res = ""
    if dir.find("?") > -1:
        parts = dir.split("?")
        if len(parts) == 2:
            dirs = parts[1].split(":")
            fd.write("!IFDEF %s\n" % parts[0])
            if len(dirs) > 0 and dirs[0].strip() != "":
                create_dir(fd, dirs[0], parts[0], i)
            else:
                empty_dir(fd, parts[0], i)
            if len(dirs) > 1 and dirs[1].strip() != "":
                fd.write("!ELSE\n")
                create_dir(fd, dirs[1], parts[0], i)
            else:
                fd.write("!ELSE\n")
                empty_dir(fd, parts[0], i)
            fd.write("!ENDIF\n")
        res = '%s-%d' % (parts[0], i)
    else:
        create_dir(fd, dir, dir, i)
        res = '%s-%d' % (dir, i)
    return res

def msc_subdirs(fd, var, values, msc):
    # to cope with conditional subdirs:
    dirs = []
    i = 0
    nvalues = []
    for dir in values:
        i = i + 1
        val = create_subdir(fd, dir, i)
        if val:
            nvalues.append(val)

    fd.write("all-recursive: %s\n" % msc_list2string(nvalues, '"', '-all" '))
    fd.write("check-recursive: %s\n" % msc_list2string(nvalues, '"', '-check" '))
    fd.write("install-recursive: %s\n" % msc_list2string(nvalues, '"', '-install" '))

def msc_assignment(fd, var, values, msc):
    o = ""
    for v in values:
        o = o + " " + v.replace('/', '\\')
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
        dir = "$(srcdir)/" + dir
    else:
        return ""
    return prefix+dir.replace('/', '\\')

def msc_translate_dir(path, msc):
    dir = path
    rest = ""
    if path.find('/') >= 0:
        dir, rest = path.split('/', 1)
    if dir == "top_builddir":
        dir = "$(TOPDIR)"
    elif dir == "top_srcdir":
        dir = "$(TOPDIR)/.."
    elif dir == "builddir":
        dir = "."
    elif dir == "srcdir":
        dir = "$(srcdir)"
    elif dir in ('bindir', 'builddir', 'datadir', 'includedir', 'infodir',
                 'libdir', 'libexecdir', 'localstatedir', 'mandir',
                 'oldincludedir', 'pkgbindir', 'pkgdatadir', 'pkgincludedir',
                 'pkglibdir', 'pkglocalstatedir', 'pkgsysconfdir', 'sbindir',
                 'sharedstatedir', 'srcdir', 'sysconfdir', 'top_builddir',
                 'top_srcdir', 'prefix'):
        dir = "$("+dir+")"
    if rest:
        dir = dir+ "\\" + rest
    return dir.replace('/', '\\')

def msc_translate_file(path, msc):
    if os.path.isfile(os.path.join(msc['cwd'], path)):
        return "$(srcdir)\\" + path
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
    while ext != "h" and f in deps:
        f = deps[f][0]
        b, ext = split_filename(f)
        if ext in automake_ext:
            pf = f
    # built source if has dep and ext != cur ext
    if pf in deps and pf not in msc['BUILT_SOURCES']:
        pfb, pfext = split_filename(pf)
        sfb, sfext = split_filename(deps[pf][0])
        if sfext != pfext:
            msc['BUILT_SOURCES'].append(pf)
    return pf

def msc_find_hdrs(target, deps, hdrs):
    base, ext = split_filename(target)
    f = target
    pf = f
    while ext != "h" and f in deps:
        f = deps[f][0]
        b, ext = split_filename(f)
        if ext in automake_ext:
            pf = f
    return pf

libno = 0
def msc_additional_libs(fd, name, sep, type, list, dlibs, msc, pref, ext):
    deps = pref+sep+name+ext+": "
    if type == "BIN":
        add = name.replace('-','_')+"_LIBS ="
    elif type == "LIB":
        add = pref+sep+name.replace('-','_')+"_LIBS ="
    else:
        add = name.replace('-','_') + " ="
    cond = ''
    for l in list:
        if '?' in l:
            c, l = l.split('?', 1)
        else:
            c = None
        d = None
        if l == "@LIBOBJS@":
            l = "$(LIBOBJS)"
        # special case (hack) for system libraries
        elif l in ('-lodbc32', '-lodbccp32', '-lversion', '-lshlwapi', '-luser32', '-llegacy_stdio_definitions'):
            l = l[2:] + '.lib'
        elif l[:2] == "-l":
            l = "lib"+l[2:]+".lib"
        elif l[:2] == "-L":
            l = '"/LIBPATH:%s"' % l[2:].replace('/', '\\')
        elif l[0] == "-":
            l = '"%s"' % l
        elif l[0] == '$':
            pass
        elif l[0] != "@":
            lib = msc_translate_dir(l, msc) + '.lib'
            # add quotes if space in name
            # we can't always add quotes since for some weird reason
            # in src\modules\plain you will then have a problem with
            # lib_algebra.lib.
            if ' ' in lib:
                lib = '"%s"' % lib
            l = lib
            d = lib
        else:
            l = None
        if c and l:
            global libno
            v = 'LIB%d' % libno
            libno = libno + 1
            cond += '!IF defined(%s)\n%s = %s\n!ELSE\n%s =\n!ENDIF\n' % (c, v, l, v)
            l = '$(%s)' % v
            if d:
                deps = '%s %s' % (deps, l)
        elif d:
            deps = '%s %s' % (deps, d)
        if l:
            add = add + ' ' + l
    # this can probably be removed...
    for l in dlibs:
        if l == "@LIBOBJS@":
            add = add + " $(LIBOBJS)"
        elif l[:2] == "-l":
            add = add + " lib"+l[2:]+".lib"
        elif l[0] in  ("-", "$"):
            add = add + " " + l
        elif l[0] not in  ("@"):
            add = add + " " + msc_translate_dir(l, msc) + ".lib"
            deps = deps + ' "' + msc_translate_dir(l, msc) + '.lib"'
    if cond:
        fd.write(cond)
    if type != "MOD":
        fd.write(deps + "\n")
    fd.write(add + "\n")

def msc_translate_ext(f):
    return f.replace('.o', '.obj')

def msc_find_target(target, msc):
    tree = msc['TREE']
    for t, v in tree.items():
        if type(v) is type({}) and 'TARGETS' in v:
            targets = v['TARGETS']
            if target in targets:
                if t == "BINS" or t[0:4] == "bin_":
                    return "BIN", "BIN"
                elif (t[0:4] == "lib_"):
                    return "LIB", t[4:].upper()
                elif t == "LIBS":
                    name, ext = split_filename(target)
                    return "LIB", name.upper()
    return "UNKNOWN", "UNKNOWN"

def msc_dep(fd, tar, deplist, msc):
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
            print("!WARNING: dropped absolute dependency " + d)
    if sep == " ":
        fd.write("\n")
    for x, y in _in:
        # TODO
        # replace this hack by something like configure ...
        fd.write('%s: "$(TOPDIR)\\winconfig_conds.py" "%s"\n' % (x, y))
        fd.write('\t$(CONFIGURE) "%s" > "%s"\n' % (y, x))
        msc['_IN'].append(y)
    getsrc = ""
    src = msc_translate_dir(msc_translate_ext(msc_translate_file(deplist[0], msc)), msc)
    if os.path.split(src)[0]:
        getsrc = '\t$(INSTALL) "%s" "%s"\n' % (src, os.path.split(src)[1])
    if ext == "tab.h":
        fd.write(getsrc)
        x, de = split_filename(deplist[0])
        of = b + '.' + de
        of = msc_translate_file(of, msc)
        fd.write('\t$(YACC) $(YFLAGS) $(AM_YFLAGS) "%s"\n' % of)
    if ext == "tab.c":
        fd.write(getsrc)
        x, de = split_filename(deplist[0])
        of = b + '.' + de
        of = msc_translate_file(of, msc)
        fd.write('\t$(YACC) $(YFLAGS) $(AM_YFLAGS) "%s"\n' % of)
    if ext == "yy.c":
        fd.write(getsrc)
        fd.write('\t$(LEX) $(LFLAGS) $(AM_LFLAGS) "%s.l"\n' % b)
    if ext in ("obj", "tab.obj", "yy.obj"):
        target, name = msc_find_target(tar, msc)
        if name[0] == '_':
            name = name[1:]
        if target == "LIB":
            d, dext = split_filename(deplist[0])
            if dext in ("c", "yy.c", "tab.c"):
                fd.write('\t$(CC) $(CFLAGS) $(%s_CFLAGS) $(GENDLL) -D_CRT_SECURE_NO_WARNINGS -DLIB%s -Fo"%s" -c "%s"\n' %
                         (split_filename(msc_basename(src))[0], name, t, src))
    if ext == 'res':
        fd.write("\t$(RC) -fo%s %s\n" % (t, src))

def msc_deps(fd, deps, objext, msc):
    if not msc['DEPS']:
        for t, deplist in deps.items():
            msc_dep(fd, t, deplist, msc)

    msc['DEPS'].append("DONE")

# list of scripts to install
def msc_scripts(fd, var, scripts, msc):

    s, ext = var.split('_', 1);
    ext = [ ext ]
    if "EXT" in scripts:
        ext = scripts["EXT"] # list of extentions

    sd = "bindir"
    if "DIR" in scripts:
        sd = scripts["DIR"][0] # use first name given
    sd = msc_translate_dir(sd, msc)

    for script in scripts['TARGETS']:
        s,ext2 = rsplit_filename(script)
        if not ext2 in ext:
            continue
        if script in msc['INSTALL']:
            continue
        if os.path.isfile(os.path.join(msc['cwd'], script+'.in')):
            inf = '$(srcdir)\\%s.in' % script
            if inf not in msc['_IN']:
                # TODO
                # replace this hack by something like configure ...
                fd.write('%s: "$(TOPDIR)\\winconfig_conds.py" "%s"\n' % (script, inf))
                fd.write('\t$(CONFIGURE) "%s" > "%s"\n' % (inf, script))
                msc['_IN'].append(inf)
        elif os.path.isfile(os.path.join(msc['cwd'], script)):
            fd.write('%s: "$(srcdir)\\%s"\n' % (script, script))
            fd.write('\t$(INSTALL) "$(srcdir)\\%s" "%s"\n' % (script, script))
        if 'COND' in scripts:
            condname = 'defined(' + ') && defined('.join(scripts['COND']) + ')'
            mkname = script.replace('.', '_').replace('-', '_')
            fd.write('!IF %s\n' % condname)
            fd.write('C_%s = %s\n' % (mkname, script))
            fd.write('!ELSE\n')
            fd.write('C_%s =\n' % mkname)
            fd.write('!ENDIF\n')
            cscript = '$(C_%s)' % mkname
        else:
            cscript = script
            condname = ''
        if not 'NOINST' in scripts and not 'NOINST_MSC' in scripts:
            msc['INSTALL'][script] = cscript, '', sd, '', condname
        msc['SCRIPTS'].append(cscript)

##    msc_deps(fd, scripts['DEPS'], "\.o", msc)

# return the unique elements of the argument list
def uniq(l):
    try:
        return list(set(l))
    except NameError:                   # presumably set() is unknown
        u = {}
        for x in l:
            u[x] = 1
        return u.keys()

# list of headers to install
def msc_headers(fd, var, headers, msc):

    sd = "includedir"
    if "DIR" in headers:
        sd = headers["DIR"][0] # use first name given
    sd = msc_translate_dir(sd, msc)

    hdrs_ext = headers['HEADERS']
    deps = []
    for d,srcs in headers['DEPS'].items():
        for s in srcs:
            if s in headers['SOURCES']:
                deps.append(d)
                break
    for header in uniq(headers['TARGETS'] + deps):
        h, ext = split_filename(header)
        if ext in hdrs_ext:
            if os.path.isfile(os.path.join(msc['cwd'], header+'.in')):
                inf = '$(srcdir)\\%s.in' % header
                if inf not in msc['_IN']:
                    # TODO
                    # replace this hack by something like configure ...
                    fd.write('%s: "$(TOPDIR)\\winconfig_conds.py" "%s"\n' % (header, inf))
                    fd.write('\t$(CONFIGURE) "%s" > "%s"\n' % (inf, header))
                    msc['_IN'].append(inf)
            elif os.path.isfile(os.path.join(msc['cwd'], header)):
                fd.write('%s: "$(srcdir)\\%s"\n' % (header, header))
##                fd.write('\t$(INSTALL) "$(srcdir)\\%s" "%s"\n' % (header, header))
##                fd.write('\tif not exist "%s" if exist "$(srcdir)\\%s" $(INSTALL) "$(srcdir)\\%s" "%s"\n' % (header, header, header, header))
                fd.write('\t$(INSTALL) "$(srcdir)\\%s" "%s"\n' % (header, header))
            if 'COND' in headers:
                condname = 'defined(' + ') && defined('.join(headers['COND']) + ')'
                mkname = header.replace('.', '_').replace('-', '_')
                fd.write('!IF %s\n' % condname)
                fd.write('C_%s = %s\n' % (mkname, header))
                fd.write('!ELSE\n')
                fd.write('C_%s =\n' % mkname)
                fd.write('!ENDIF\n')
                cheader = '$(C_%s)' % mkname
            else:
                cheader = header
                condname = ''
            msc['INSTALL'][header] = header, '', sd, '', condname
            msc['SCRIPTS'].append(header)
            if header not in headers['SOURCES']:
                msc['BUILT_SOURCES'].append(cheader)

##    msc_find_ins(msc, headers)
##    msc_deps(fd, headers['DEPS'], "\.o", msc)

def msc_binary(fd, var, binmap, msc):

    if type(binmap) == type([]):
        name = var[4:]
        if name == 'SCRIPTS':
            for i in binmap:
                if os.path.isfile(os.path.join(msc['cwd'], i+'.in')):
                    # TODO
                    # replace this hack by something like configure ...
                    fd.write('%s: "$(TOPDIR)\\winconfig_conds.py" "$(srcdir)\\%s.in"\n' % (i, i))
                    fd.write('\t$(CONFIGURE) "$(srcdir)\\%s.in" > "%s"\n' % (i, i))
                elif os.path.isfile(os.path.join(msc['cwd'], i)):
                    fd.write('%s: "$(srcdir)\\%s"\n' % (i, i))
                    fd.write('\t$(INSTALL) "$(srcdir)\\%s" "%s"\n' % (i, i))
                msc['INSTALL'][i] = i, '', '$(bindir)', '', ''
        else: # link
            binmap = binmap[0]
            if '?' in binmap:
                cond, binmap = binmap.split('?', 1)
                condname = 'defined(%s)' % cond
            else:
                cond = condname = ''
            src = binmap[4:]
            if cond:
                fd.write('!IF %s\n' % condname)
            fd.write('%s: "%s"\n' % (name, src))
            fd.write('\t$(CP) "%s" "%s"\n\n' % (src, name))
            if cond:
                fd.write('!ELSE\n')
                fd.write('%s:\n' % name)
                fd.write('!ENDIF\n')
            n_nme, n_ext = split_filename(name)
            s_nme, s_ext = split_filename(src)
            if n_ext  and  s_ext  and  n_ext == s_ext:
                ext = ''
            else:
                ext = '.exe'
            msc['INSTALL'][name] = src + ext, ext, '$(bindir)', '', condname
            msc['SCRIPTS'].append(name)
        return

    if 'MTSAFE' in binmap:
        fd.write("CFLAGS=$(CFLAGS) $(thread_safe_flag_spec)\n")

    HDRS = []
    hdrs_ext = []
    if 'HEADERS' in binmap:
        hdrs_ext = binmap['HEADERS']

    SCRIPTS = []
    scripts_ext = []
    if 'SCRIPTS' in binmap:
        scripts_ext = binmap['SCRIPTS']

    name = var[4:]
    if "NAME" in binmap:
        binname = binmap['NAME'][0]
    else:
        binname = name
    binname2 = binname.replace('-','_').replace('.', '_')

    if 'COND' in binmap:
        condname = 'defined(' + ') && defined('.join(binmap['COND']) + ')'
        fd.write('!IF %s\n' % condname)
        fd.write('C_%s_exe = %s.exe\n' % (binname2, binname))
        msc['BINS'].append((binname, '$(C_%s_exe)' % binname2, condname))
    elif 'CONDINST' in binmap:
        condname = 'defined(' + ') && defined('.join(binmap['CONDINST']) + ')'
        fd.write('!IF %s\n' % condname)
        fd.write('C_inst_%s_exe = %s.exe\n' % (binname2, binname))
        fd.write('C_noinst_%s_exe = \n' % (binname2))
        fd.write('!ELSE\n')
        fd.write('C_inst_%s_exe = \n' % (binname2))
        fd.write('C_noinst_%s_exe = %s.exe\n' % (binname2, binname))
        fd.write('!ENDIF\n')
        msc['BINS'].append((binname, '$(C_inst_%s_exe)' % binname2, condname))
        condname = '!defined(' + ') && !defined('.join(binmap['CONDINST']) + ')'
        msc['NBINS'].append((binname, '$(C_noinst_%s_exe)' % binname2, condname))
    else:
        condname = ''
        if 'NOINST' in binmap:
            msc['NBINS'].append((binname, binname, condname))
        else:
            msc['BINS'].append((binname, binname, condname))

    if "DIR" in binmap:
        bd = binmap["DIR"][0] # use first name given
        fd.write("%sdir = %s\n" % (binname2, msc_translate_dir(bd,msc)) );
    else:
        fd.write("%sdir = $(bindir)\n" % binname2);

    binlist = []
    if "LIBS" in binmap:
        binlist = binlist + binmap["LIBS"]
    if "WINLIBS" in binmap:
        binlist = binlist + binmap["WINLIBS"]
    if binlist:
        msc_additional_libs(fd, binname, "", "BIN", binlist, [], msc, '', '.exe')

    srcs = binname2+"_OBJS ="
    for target in binmap['TARGETS']:
        t, ext = split_filename(target)
        if ext == "o":
            srcs = srcs + " " + t + ".obj"
        elif ext == "tab.o":
            srcs = srcs + " " + t + ".tab.obj"
        elif ext == "yy.o":
            srcs = srcs + " " + t + ".yy.obj"
        elif ext == 'def':
            srcs = srcs + ' ' + target
        elif ext == 'res':
            srcs = srcs + " " + t + ".res"
        elif ext in hdrs_ext:
            HDRS.append(target)
        elif ext in scripts_ext:
            if target not in SCRIPTS:
                SCRIPTS.append(target)
    fd.write(srcs + "\n")
    fd.write("%s.exe: $(%s_OBJS)\n" % (binname, binname2))
    fd.write('\t$(CC) $(CFLAGS)')
    fd.write(" -Fe%s.exe $(%s_OBJS) /link $(%s_LIBS) /subsystem:console /NODEFAULTLIB:LIBC\n" % (binname, binname2, binname2))
    fd.write("\t$(EDITBIN) $@ /HEAP:1048576,1048576 /LARGEADDRESSAWARE\n");
    fd.write("\tif exist $@.manifest $(MT) -manifest $@.manifest -outputresource:$@;1\n");
    if condname:
        fd.write('!ELSE\n')
        fd.write('C_%s_exe =\n' % binname2)
        fd.write('!ENDIF\n')
    fd.write('\n')

    if SCRIPTS:
        fd.write(binname2+"_SCRIPTS =" + msc_space_sep_list(SCRIPTS))
        msc['BUILT_SOURCES'].append("$(" + binname2 + "_SCRIPTS)")

    if 'HEADERS' in binmap:
        for h in HDRS:
            msc['HDRS'].append(h)

    msc_deps(fd, binmap['DEPS'], ".obj", msc)

def msc_bins(fd, var, binsmap, msc):

    HDRS = []
    hdrs_ext = []
    if 'HEADERS' in binsmap:
        hdrs_ext = binsmap['HEADERS']

    SCRIPTS = []
    scripts_ext = []
    if 'SCRIPTS' in binsmap:
        scripts_ext = binsmap['SCRIPTS']

    name = ""
    if "NAME" in binsmap:
        name = binsmap["NAME"][0] # use first name given
    if 'MTSAFE' in binsmap:
        fd.write("CFLAGS=$(CFLAGS) $(thread_safe_flag_spec)\n")

    for binsrc in binsmap['SOURCES']:
        bin, ext = split_filename(binsrc)
        #if ext not in automake_ext:
        msc['EXTRA_DIST'].append(binsrc)
        bin2 = bin.replace('-','_')

        if "DIR" in binsmap:
            bd = binsmap["DIR"][0] # use first name given
            fd.write("%sdir = %s\n" % (bin2, msc_translate_dir(bd,msc)) );
        else:
            fd.write("%sdir = $(bindir)\n" % (bin2) );

        if 'CONDINST' in binsmap:
            condname = 'defined(' + ') && defined('.join(binsmap['CONDINST']) + ')'
            fd.write('!IF %s\n' % condname)
            fd.write('C_inst_%s_exe = %s.exe\n' % (bin2, bin))
            fd.write('C_noinst_%s_exe = \n' % (bin2))
            fd.write('!ELSE\n')
            fd.write('C_inst_%s_exe = \n' % (bin2))
            fd.write('C_noinst_%s_exe = %s.exe\n' % (bin2, bin))
            fd.write('!ENDIF\n')
            msc['BINS'].append((bin, '$(C_inst_%s_exe)' % bin2, condname))
            condname = '!defined(' + ') && !defined('.join(binsmap['CONDINST']) + ')'
            msc['NBINS'].append((bin, '$(C_noinst_%s_exe)' % bin2, condname))
        elif 'NOINST' in binsmap:
            msc['NBINS'].append((bin, bin, ''))
        else:
            msc['BINS'].append((bin, bin, ''))

        if bin + "_LIBS" in binsmap:
            msc_additional_libs(fd, bin, "", "BIN", binsmap[bin + "_LIBS"], [], msc, '', '.exe')
        else:
            binslist = []
            if "LIBS" in binsmap:
                binslist = binslist + binsmap["LIBS"]
            if "WINLIBS" in binsmap:
                binslist = binslist + binsmap["WINLIBS"]
            if binslist:
                msc_additional_libs(fd, bin, "", "BIN", binslist, [], msc, '', '.exe')

        srcs = bin+"_OBJS ="
        for target in binsmap['TARGETS']:
            t, ext = split_filename(target)
            if t == bin:
                t, ext = split_filename(target)
                if ext == "o":
                    srcs = srcs + " " + t + ".obj"
                elif ext == "tab.o":
                    srcs = srcs + " " + t + ".tab.obj"
                elif ext == "yy.o":
                    srcs = srcs + " " + t + ".yy.obj"
                elif ext == 'res':
                    srcs = srcs + " " + t + ".res"
                elif ext in hdrs_ext:
                    HDRS.append(target)
                elif ext in scripts_ext:
                    if target not in SCRIPTS:
                        SCRIPTS.append(target)
        fd.write(srcs + "\n")
        fd.write("%s.exe: $(%s_OBJS)\n" % (bin, bin.replace('-','_')))
        fd.write('\t$(CC) $(CFLAGS)')
        fd.write(" -Fe%s.exe $(%s_OBJS) /link $(%s_LIBS) /subsystem:console /NODEFAULTLIB:LIBC\n" % (bin, bin.replace('-','_'), bin.replace('-','_')))
        fd.write("\tif exist $@.manifest $(MT) -manifest $@.manifest -outputresource:$@;1\n\n");

    if SCRIPTS:
        fd.write(name.replace('-','_')+"_SCRIPTS =" + msc_space_sep_list(SCRIPTS))
        msc['BUILT_SOURCES'].append("$(" + name.replace('-','_') + "_SCRIPTS)")

    if 'HEADERS' in binsmap:
        for h in HDRS:
            msc['HDRS'].append(h)

    msc_deps(fd, binsmap['DEPS'], ".obj", msc)

def msc_mods_to_libs(fd, var, modmap, msc):
    modname = var[:-4]+"LIBS"
    msc_assignment(fd, var, modmap, msc)
    msc_additional_libs(fd, modname, "", "MOD", modmap, [], msc, 'lib', '.dll')

def msc_library(fd, var, libmap, msc):

    name = var[4:]
    sep = ""
    pref = 'lib'
    dll = '.dll'
    if "NAME" in libmap:
        libname = libmap['NAME'][0]
    else:
        libname = name

    if 'PREFIX' in libmap:
        if libmap['PREFIX']:
            pref = libmap['PREFIX'][0]
        else:
            pref = ''
    instlib = 1

    if (libname[0] == "_"):
        sep = "_"
        libname = libname[1:]
    if 'SEP' in libmap:
        sep = libmap['SEP'][0]

    lib = "lib"
    ld = "LIBDIR"
    if "DIR" in libmap:
        lib = libname
        ld = libmap["DIR"][0] # use first name given
    ld = msc_translate_dir(ld,msc)

    HDRS = []
    hdrs_ext = []
    if 'HEADERS' in libmap:
        hdrs_ext = libmap['HEADERS']

    SCRIPTS = []
    scripts_ext = []
    if 'SCRIPTS' in libmap:
        scripts_ext = libmap['SCRIPTS']

    v = sep + libname
    makedll = pref + v + dll
    if 'NOINST' in libmap or 'NOINST_MSC' in libmap:
        if "LIBS" in libmap or "WINLIBS" in libmap:
            print("!WARNING: no sense in having a LIBS section with NOINST")
        makelib = pref + v + '.lib'
    else:
        makelib = makedll
    if 'COND' in libmap:
        condname = 'defined(' + ') && defined('.join(libmap['COND']) + ')'
        mkname = (pref + v).replace('.', '_').replace('-', '_')
        fd.write('!IF %s\n' % condname)
        fd.write('C_%s_dll = %s%s%s\n' % (mkname, pref, v, dll))
        fd.write('C_%s_lib = %s%s.lib\n' % (mkname, pref, v))
        fd.write('!ELSE\n')
        fd.write('C_%s_dll =\n' % mkname)
        fd.write('C_%s_lib =\n' % mkname)
        fd.write('!ENDIF\n')
        makelib = '$(C_%s_lib)' % mkname
        makedll = '$(C_%s_dll)' % mkname
    else:
        condname = ''

    if 'NOINST' in libmap or 'NOINST_MSC' in libmap:
        msc['NLIBS'].append(makelib)
    else:
        msc['LIBS'].append(makelib)
        if instlib:
            i = pref + v + '.lib'
        else:
            i = ''
        if ld != 'LIBDIR':
            msc['INSTALL'][pref + v] = makedll, dll, ld, i, condname
        else:
            msc['INSTALL'][pref + v] = makedll, dll, '$(%sdir)' % lib.replace('-', '_'), i, condname

    if 'MTSAFE' in libmap:
        fd.write("CFLAGS=$(CFLAGS) $(thread_safe_flag_spec)\n")

    dlib = []
    if libname+ "_DLIBS" in libmap:
        dlib = libmap[libname+"_DLIBS"]
    liblist = []
    if "LIBS" in libmap:
        liblist = liblist + libmap["LIBS"]
    if "WINLIBS" in libmap:
        liblist = liblist + libmap["WINLIBS"]
    if liblist:
        msc_additional_libs(fd, libname, sep, "LIB", liblist, dlib, msc, pref, dll)

    for src in libmap['SOURCES']:
        base, ext = split_filename(src)
        #if ext not in automake_ext:
        msc['EXTRA_DIST'].append(src)

    srcs = '%s%s%s_OBJS =' % (pref, sep, libname)
    deps = '%s%s%s_DEPS = $(%s%s%s_OBJS)' % (pref, sep, libname, pref, sep, libname)
    for dep in libmap.get('XDEPS', []):
        deps = deps + ' ' + dep
    deffile = ''
    for target in libmap['TARGETS']:
        if target == "@LIBOBJS@":
            srcs = srcs + " $(LIBOBJS)"
        else:
            t, ext = split_filename(target)
            if ext == "o":
                srcs = srcs + " " + t + ".obj"
            elif ext == "tab.o":
                srcs = srcs + " " + t + ".tab.obj"
            elif ext == "yy.o":
                srcs = srcs + " " + t + ".yy.obj"
            elif ext == "pm.o":
                srcs = srcs + " " + t + ".pm.obj"
            elif ext == 'res':
                srcs = srcs + " " + t + ".res"
            elif ext in hdrs_ext:
                HDRS.append(target)
            elif ext in scripts_ext:
                if target not in SCRIPTS:
                    SCRIPTS.append(target)
            elif ext == 'def':
                deffile = ' "-DEF:%s"' % target
                deps = deps + " " + target
    fd.write(srcs + "\n")
    fd.write(deps + "\n")
    ln = pref + sep + libname
    if 'NOINST' in libmap or 'NOINST_MSC' in libmap:
        ln_ = ln.replace('-','_')
        fd.write("%s.lib: $(%s_DEPS)\n" % (ln, ln_))
        fd.write('\t$(ARCHIVER) /out:"%s.lib" $(%s_OBJS) $(%s_LIBS)\n' % (ln, ln_, ln_))
    else:
        fd.write("%s.lib: %s%s\n" % (ln, ln, dll))
        fd.write("%s%s: $(%s_DEPS) \n" % (ln, dll, ln.replace('-','_')))
        fd.write('\tpython "$(TOPDIR)\\..\\NT\\wincompile.py" $(CC) $(CFLAGS) -LD -Fe%s%s @<< /link @<<\n$(%s_OBJS)\n<<\n$(%s_LIBS)%s\n<<\n' % (ln, dll, ln.replace('-','_'), ln.replace('-','_'), deffile))
        fd.write("\tif exist $@.manifest $(MT) -manifest $@.manifest -outputresource:$@;2\n");
        if sep == '_':
            fd.write('\tif not exist .libs $(MKDIR) .libs\n')
            fd.write('\t$(INSTALL) "%s%s" ".libs\\%s%s"\n' % (ln, dll, ln, dll))
    fd.write("\n")

    if SCRIPTS:
        fd.write(libname.replace('-','_')+"_SCRIPTS =" + msc_space_sep_list(SCRIPTS))
        msc['BUILT_SOURCES'].append("$(" + name.replace('-','_') + "_SCRIPTS)")

    if 'HEADERS' in libmap:
        for h in HDRS:
            msc['HDRS'].append(h)

    msc_deps(fd, libmap['DEPS'], ".obj", msc)

def msc_includes(fd, var, values, msc):
    incs = "-I$(srcdir)"
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

output_funcs = {'SUBDIRS': msc_subdirs,
                'EXTRA_DIST': msc_extra_dist,
                'EXTRA_HEADERS': msc_extra_headers,
                'LIBDIR': msc_libdir,
                'LIB': msc_library,
                'BINS': msc_bins,
                'BIN': msc_binary,
                'SCRIPTS': msc_scripts,
                'INCLUDES': msc_includes,
                'MTSAFE': msc_mtsafe,
                'CFLAGS': msc_cflags,
                'STATIC_MODS': msc_mods_to_libs,
                'smallTOC_SHARED_MODS': msc_mods_to_libs,
                'largeTOC_SHARED_MODS': msc_mods_to_libs,
                'HEADERS': msc_headers,
                }

def output(tree, cwd, topdir):
    # HACKS to keep uncompilable stuff out of Windows makefiles.
    todelete = []
    for k, v in tree.items():
        if type(v) is type({}):
            if 'COND' in v:
                if 'NOT_WIN32' in v['COND']:
                    todelete += [ k ]
    for k in todelete:
        del tree[k]

    fd = open(os.path.join(cwd, 'Makefile.msc'), "w")

    fd.write(MAKEFILE_HEAD)

    if 'INCLUDES' not in tree:
        tree.add('INCLUDES', [])

    msc = {}
    msc['BUILT_SOURCES'] = []
    msc['EXTRA_DIST'] = []
    msc['LIBS'] = []            # libraries which are installed (DLLs)
    msc['NLIBS'] = []           # archives (libraries which are not installed)
    msc['SCRIPTS'] = []
    msc['BINS'] = []
    msc['NBINS'] = []
    msc['HDRS'] = []
    msc['INSTALL'] = {}
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

    fd.write("TOPDIR = %s\n" % reldir.replace('/', '\\'))
    fd.write("srcdir = $(TOPDIR)\\..%s\n" % srcdir.replace('/', '\\'))
    fd.write("!INCLUDE $(TOPDIR)\\..\\NT\\rules.msc\n")
    if 'SUBDIRS' in tree:
        fd.write("all: build-all\n")
        fd.write("check: check-recursive check-msc\n")
        fd.write("install: install-recursive install-msc\n")
    else:
        fd.write("all: all-msc\n")
        fd.write("check: check-msc\n")
        fd.write("install: install-msc\n")

    for i, v in tree.items():
        j = i
        if i.find('_') >= 0:
            k, j = i.split('_', 1)
            j = k.upper()
        if type(v) is type([]):
            if i in output_funcs:
                output_funcs[i](fd, i, v, msc)
            elif j in output_funcs:
                output_funcs[j](fd, i, v, msc)
            elif i != 'TARGETS':
                msc_assignment(fd, i, v, msc)

    for i, v in tree.items():
        j = i
        if i.find('_') >= 0:
            k, j = i.split('_', 1)
            j = k.upper()
        if type(v) is type({}):
            if i in output_funcs:
                output_funcs[i](fd, i, v, msc)
            elif j in output_funcs:
                output_funcs[j](fd, i, v, msc)
            elif i != 'TARGETS':
                msc_assignment(fd, i, v, msc)

    if msc['BUILT_SOURCES']:
        fd.write("BUILT_SOURCES = ")
        for v in msc['BUILT_SOURCES']:
            fd.write(" %s" % v)
        fd.write("\n")

    if 'SUBDIRS' in tree:
        fd.write('build-all: $(BUILT_SOURCES) all-recursive all-msc\n')

##    fd.write("EXTRA_DIST = Makefile.ag Makefile.msc")
##    for v in msc['EXTRA_DIST']:
##        fd.write(" %s" % v)
##    fd.write("\n")

    fd.write("all-msc:")
    if msc['LIBS']:
        for v in msc['LIBS']:
            if v[:1] == '$':
                fd.write(' %s' % v)
            else:
                fd.write(' "%s"' % v)

    if msc['NLIBS']:
        for v in msc['NLIBS']:
            if v[:1] == '$':
                fd.write(' %s' % v)
            else:
                fd.write(' "%s"' % v)

    if msc['NBINS']:
        for x, v, cond in msc['NBINS']:
            if v[:1] == '$':
                fd.write(' %s' % v)
            else:
                fd.write(' "%s.exe"' % v)

    if msc['BINS']:
        for x, v, cond in msc['BINS']:
            if v[:1] == '$':
                fd.write(' %s' % v)
            else:
                fd.write(' "%s.exe"' % v)

    if msc['SCRIPTS']:
        for v in msc['SCRIPTS']:
            if v[:1] == '$':
                fd.write(' %s' % v)
            else:
                fd.write(' "%s"' % v)

    if cwd != topdir and msc['HDRS']:
        for v in msc['HDRS']:
            if v[:1] == '$':
                fd.write(' %s' % v)
            else:
                fd.write(' "%s"' % v)

    fd.write("\n")

    fd.write("check-msc: all-msc")
    if msc['INSTALL']:
        for dst, (src, ext, dir, instlib, cond) in msc['INSTALL'].items():
            if src[:1] == '$':
                fd.write(' %s' % src)
            else:
                fd.write(' "%s"' % src)
    fd.write("\n")

    fd.write("install-msc: install-exec install-data\n")
    l = []
    for dst, (src, ext, dir, instlib, cond) in msc['INSTALL'].items():
        l.append(dst)
    b = []
    for dst, src, cond in msc['BINS']:
        b.append(dst)

    fd.write("install-exec: %s %s\n" % (
        msc_list2string(b, '"install_bin_','" '),
        msc_list2string(l, '"install_','" ')))
    if msc['BINS']:
        for dst, src, cond in msc['BINS']:
            if cond:
                fd.write('!IF %s\n' % cond)
            if src[:1] != '$':
                src = '"%s.exe"' % src
            fd.write('install_bin_%s: %s\n' % (dst, src))
            fd.write('\tif not exist "$(%sdir)" $(MKDIR) "$(%sdir)"\n' % (dst.replace('-','_'), dst.replace('-','_')))
            fd.write('\t$(INSTALL) %s "$(%sdir)"\n' % (src,dst.replace('-','_')))
            if cond:
                fd.write('!ELSE\n')
                fd.write('install_bin_%s:\n' % dst)
                fd.write('!ENDIF\n')
    if msc['INSTALL']:
        for dst, (src, ext, dir, instlib, cond) in msc['INSTALL'].items():
            if not dir:
                continue
            if cond:
                fd.write('!IF %s\n' % cond)
            fd.write('install_%s: "%s" "%s"\n' % (dst, src, dir))
            fd.write('\t$(INSTALL) "%s" "%s\\%s%s"\n' % (src, dir, dst, ext))
            if instlib:
                fd.write('\t$(INSTALL) "%s" "%s\\%s%s"\n' % (instlib, dir, dst, '.lib'))
            if cond:
                fd.write('!ELSE\n')
                fd.write('install_%s:\n' % dst)
                fd.write('!ENDIF\n')
        td = {}
        for dst, (src, ext, dir, instlib, cond) in msc['INSTALL'].items():
            if not dir:
                continue
            if not dir in td:
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

# vim:ts=4 sw=4 expandtab:
