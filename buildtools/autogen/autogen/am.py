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
# Portions created by CWI are Copyright (C) 1997-2005 CWI.
# All Rights Reserved.

import string
import os
from codegen import find_org

#automake_ext = ['c', 'cc', 'h', 'y', 'yy', 'l', 'll', 'glue.c']
automake_ext = ['c', 'cc', 'h', 'tab.c', 'tab.cc', 'tab.h', 'yy.c', 'yy.cc', 'glue.c', 'proto.h', 'py.c', 'pm.c', '']
am_assign = "+="

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

def cond_subdir(fd, dir, i):
    res = ""
    parts = string.split(dir, "?")
    if len(parts) == 2:
        dirs = string.split(parts[1], ":")
        fd.write("if %s\n" % parts[0])
        if len(dirs) > 0 and string.strip(dirs[0]) != "":
            fd.write("%s_%d_SUBDIR = %s\n" % (parts[0], i, dirs[0]))
        else:
            fd.write("%s_%d_SUBDIR = \n" % (parts[0], i))
        if len(dirs) > 1 and string.strip(dirs[1]) != "":
            fd.write("else\n")
            fd.write("%s_%d_SUBDIR = %s\n" % (parts[0], i, dirs[1]))
        else:
            fd.write("else\n")
            fd.write("%s_%d_SUBDIR = \n" % (parts[0], i))
        fd.write("endif\n")
        res = "$(" + parts[0] + "_" + str(i) + "_SUBDIR)"
    return res

def am_sort_libs(libs, tree):
    res = []
    for (pref,lib,sep,cond) in libs:
        after = -1
        # does lib depend on a other library
        if tree.has_key('lib_'+ lib):
            v = tree['lib_'+lib]
            if v.has_key("LIBS"):
                for l in v['LIBS']:
                    if len(l) > 3:
                        l = l[3:] # strip lib prefix
                    if l in res:
                        pos = res.index(l)
                        if pos > after:
                            after = pos
        elif tree.has_key('LIBS'):
            v = tree['LIBS']
            if v.has_key(lib[1:] + "_DLIBS"):
                for l in v[lib[1:] + '_DLIBS']:
                    if len(l) > 3:
                        l = l[3:] # strip lib prefix
                    if l in res:
                        pos = res.index(l)
                        if pos > after:
                            after = pos
        res.insert(after + 1, (pref, lib, sep, cond))
    return res

def am_subdirs(fd, var, values, am):
    dirs = []
    i = 0
    for dir in values:
        i = i + 1
        if string.find(dir, "?") > -1:
            dirs.append(cond_subdir(fd, dir, i))
        else:
            dirs.append(dir)

    am_assignment(fd, var, dirs, am)

def am_assignment(fd, var, values, am):
    o = ""
    for v in values:
        o = o + " " + am_translate_dir(v, am)
    fd.write("%s = %s\n" % (var, o))

def am_cflags(fd, var, values, am):
    o = ""
    for v in values:
        o = o + " " + v
    fd.write("%s %s %s\n" % (var, am_assign, o))

def am_extra_dist(fd, var, values, am):
    for i in values:
        am['EXTRA_DIST'].append(i)
        t, ext = rsplit_filename(i)
        if ext == 'in':
            am['OutList'].append(am['CWD']+t)

def am_extra_dist_dir(fd, var, values, am):
    fd.write("dist-hook:\n")
    for i in values:
        fd.write("\tmkdir -p $(distdir)/%s\n" % i)
        fd.write("\tcp -R $(srcdir)/%s/* $(distdir)/%s\n" % (i, i))
        fd.write("\trm -rf $(distdir)/%s/CVS\n" % i)

def am_extra_headers(fd, var, values, am):
    for i in values:
        am['HDRS'].append(i)

def am_libdir(fd, var, values, am):
    am['LIBDIR'] = values[0]

def am_mtsafe(fd, var, values, am):
    fd.write("CFLAGS %s $(THREAD_SAVE_FLAGS)\n" % am_assign)
    fd.write("CXXFLAGS %s $(THREAD_SAVE_FLAGS)\n" % am_assign)

def am_list2string(l, pre, post):
    res = ""
    for i in l:
        res = res + pre + i + post
    return res

def am_find_srcs(target, deps, am):
    dist = 1;
    base, ext = split_filename(target)
    f = target
    pf = f
    while ext != "h" and deps.has_key(f):
        f = deps[f][0]
        b, ext = split_filename(f)
        if ext in automake_ext:
            pf = f

    # built source if has dep and ext != cur ext
    if deps.has_key(pf) and pf not in am['BUILT_SOURCES']:
        pfb, pfext = split_filename(pf)
        sfb, sfext = split_filename(deps[pf][0])
        if sfext != pfext:
            if pfext in automake_ext:
                dist = None
                am['BUILT_SOURCES'].append(pf)
    b, ext = split_filename(pf)
    if ext in automake_ext:
        return (dist,pf)
    return (dist,"")

def am_find_hdrs_r(am, target, deps, hdrs, hdrs_ext, map):
    if deps.has_key(target):
        tdeps = deps[target]
        for dtarget in tdeps:
            org = find_org(deps, dtarget)
            if org in map['SOURCES']:
                t, ext = split_filename(dtarget)
                if ext in hdrs_ext and not dtarget in hdrs:
                    hdrs.append(dtarget)
                am_find_hdrs_r(am, dtarget, deps, hdrs, hdrs_ext, map)

def am_find_hdrs(am, map):
    if map.has_key('HEADERS'):
        hdrs_ext = map['HEADERS']
        for target in map['TARGETS']:
            t, ext = split_filename(target)
            if ext in hdrs_ext and not target in am['HDRS']:
                am['HDRS'].append(target)
            am_find_hdrs_r(am, target, map['DEPS'], am['HDRS'], hdrs_ext, map)

def am_find_ins(am, map):
    for source in map['SOURCES']:
        t, ext = rsplit_filename(source)
        if ext == 'in':
            am['OutList'].append(am['CWD']+t)

def am_additional_flags(name, sep, type, list, am):
    if type == "BIN":
        add = am_normalize(name)+"_LDFLAGS ="
    elif type == "LIB":
        add = "lib"+sep+name+"_la_LDFLAGS ="
    else:
        add = name + " ="
    for l in list:
        add = add + " " + l
    return add + "\n"

def am_additional_libs(name, sep, type, list, am, pref = 'lib'):
    if type == "BIN":
        add = am_normalize(name)+"_LDADD ="
    elif type == "LIB":
        add = pref+sep+name+"_la_LIBADD ="
    else:
        add = name + " ="
    for l in list:
        if l[0] in ("-", "$", "@"):
            add = add + " " + l
        else:
            add = add + " " + am_translate_dir(l, am) + ".la"
    return add + "\n"

def am_additional_install_libs(name, sep, list, am):
    add = "$(do)install-" + name + "LTLIBRARIES : "
    for l in list:
        if l[0] not in ("-", "$", "@", "." ):
            if l[3] == '_':
                l = l[4:]
            else:
                l = l[3:]
            add = add + " install-" + l + "LTLIBRARIES"
    return add + "\n"

def am_deps(fd, deps, objext, am):
    if len(am['DEPS']) <= 0:
        for t, deplist in deps.items():
            t = t.replace('\\', '/')
            n = t.replace('.o', '.lo', 1)
            if t != n:
                fd.write(n + " ")
            fd.write(t + ":")
            for d in deplist:
                if not os.path.isabs(d):
                    fd.write(" " + am_translate_dir(d, am))
                else:
                    print("!WARNING: dropped absolute dependency " + d)
            fd.write("\n")
    am['DEPS'].append("DONE")


# list of scripts to install
def am_scripts(fd, var, scripts, am):
#todo handle 'EXT' for empty ''.

    s, ext = string.split(var, '_', 1);
    ext = [ ext ]
    if scripts.has_key("EXT"):
        ext = scripts["EXT"] # list of extentions

    sd = "SCRIPTSDIR"
    if scripts.has_key("DIR"):
        sd = scripts["DIR"][0] # use first name given
    sd = am_translate_dir(sd, am)

    for src in scripts['SOURCES']:
        am['EXTRA_DIST'].append(src)

    for script in scripts['TARGETS']:
        s,ext2 = rsplit_filename(script)
        if not ext2 in ext:
            continue

        cond = ''
        s = script
        scriptname = "script_" + script
        if scripts.has_key('COND'):
            condname = string.join(scripts['COND'], '+')
            mkname = string.replace(script, '.', '_')
            cond = '#' + condname
            s = "$(C_" + mkname + ")"
            scriptname = "$(C_script_" + mkname + ")"

        name = "script_" + script
        if scriptname not in am['BIN_SCRIPTS'] and name not in am['BIN_SCRIPTS']:
            am['BIN_SCRIPTS'].append(scriptname)
        else:
            continue

        if cond:
            fd.write("uninstall-local-:\n");
            fd.write("install-exec-local-:\n");
            fd.write("if %s\n" % (condname))
            fd.write(" C_%s = %s\n" % (mkname,script))
            fd.write(" C_script_%s = script_%s\n" % (mkname, script))
            fd.write("endif\n")

        fd.write("script_%s: %s\n" % (script, script))
        fd.write("\tchmod a+x $<\n")
        if sd == "$(sysconfdir)":
            fd.write("install-exec-local-%s: %s\n" % (script, script))
            fd.write("\t-mkdir -p $(DESTDIR)%s\n" % sd)
            fd.write("\t$(INSTALL) $(INSTALL_BACKUP) $< $(DESTDIR)%s/%s\n\n" % (sd, script))
            fd.write("uninstall-local-%s: \n" % script)
            fd.write("\t$(RM) $(DESTDIR)%s/%s\n\n" % (sd, script))
        else:
            fd.write("install-exec-local-%s: %s\n" % (script, script))
            fd.write("\t-mkdir -p $(DESTDIR)%s\n" % sd)
            fd.write("\t-$(RM) $(DESTDIR)%s/%s\n" % (sd, script))
            fd.write("\t$(INSTALL) $< $(DESTDIR)%s/%s\n\n" % (sd, script))
            fd.write("uninstall-local-%s: \n" % script)
            fd.write("\t$(RM) $(DESTDIR)%s/%s\n\n" % (sd, script))

        am['INSTALL'].append(s)
        am['UNINSTALL'].append(s)
        am['InstallList'].append("\t"+sd+"/"+script+cond+"\n")

    am_find_ins(am, scripts)
    am_deps(fd, scripts['DEPS'], "\.o", am)

# list of headers to install
def am_headers(fd, var, headers, am):

    sd = "HEADERSDIR"
    if headers.has_key("DIR"):
        sd = headers["DIR"][0] # use first name given
    sd = am_translate_dir(sd, am)

    hdrs_ext = headers['HEADERS']
    for header in headers['TARGETS']:
        h, ext = split_filename(header)
        if ext in hdrs_ext:
            fd.write("install-exec-local-%s: %s\n" % (header, header))
            fd.write("\t-mkdir -p $(DESTDIR)%s\n" % sd)
            fd.write("\t-$(RM) $(DESTDIR)%s/%s\n" % (sd, header))
            fd.write("\t$(INSTALL_DATA) $< $(DESTDIR)%s/%s\n\n" % (sd, header))
            fd.write("uninstall-local-%s: \n" % header)
            fd.write("\t$(RM) $(DESTDIR)%s/%s\n\n" % (sd, header))
            am['INSTALL'].append(header)
            am['UNINSTALL'].append(header)
            cond = ''
            if headers.has_key('COND'):
                cond = '#' + string.join(headers['COND'], '+')
            am['InstallList'].append("\t"+sd+"/"+header+cond+"\n")

    am_find_ins(am, headers)
    am_deps(fd, headers['DEPS'], "\.o", am)
    for src in headers['SOURCES']:
        am['EXTRA_DIST'].append(src)

def am_doc(fd, var, docmap, am):
    docdir = "pkgdatadir"
    if docmap.has_key("DIR"):
        docdir = docmap["DIR"][0] # use first name given
    docdir = am_translate_dir(docdir, am)

    name = var[4:]
    if name[0] == "_":
        name = name[1:]

    doc_ext = ['pdf', 'ps', 'bdy.tex', 'bdy.html', 'html']

    srcs = name+"_DOCS ="
    for target in docmap['TARGETS']:
        t, ext = split_filename(target)
        if ext in doc_ext:
            srcs = srcs + " " + target
            am['DocList'].append("\t"+docdir+"/"+target+"\n")
    fd.write(srcs + "\n")

    fd.write("if DOCTOOLS\n")
    fd.write("all-local-%s: $(%s_DOCS)\n" % (name, name))
    fd.write("install-data-local-%s: $(%s_DOCS)\n" % (name, name))
    fd.write("\t-mkdir -p $(DESTDIR)%s\n" % docdir)
    fd.write("\t$(INSTALL) $(%s_DOCS) $(DESTDIR)%s\n" % (name, docdir))
    fd.write("uninstall-local-%s: \n" % name)
    fd.write("\tcd $(DESTDIR)%s; $(RM) $(%s_DOCS)\n" % (docdir, name))
    fd.write("else\n")
    fd.write("all-local-%s: \n" % name)
    fd.write("install-data-local-%s: \n" % name)
    fd.write("uninstall-local-%s: \n" % name)
    fd.write("endif\n")
    am['ALL'].append(name)
    am['DATA_INSTALL'].append(name)
    am['UNINSTALL'].append(name)

    am_find_ins(am, docmap)
    am_deps(fd, docmap['DEPS'], "\.o", am)

def am_normalize(name):
    return string.replace(name, '-', '_')

def am_binary(fd, var, binmap, am):

    if type(binmap) == type([]):
        name = var[4:]
        if name == 'SCRIPTS':
            for script in binmap:
                if script not in am['BIN_SCRIPTS']:
                    am['BIN_SCRIPTS'].append(script)
            am['INSTALL'].append(name)
            am['UNINSTALL'].append(name)
            am['ALL'].append(name)
            for i in binmap:
                am['InstallList'].append("\t$(bindir)/"+i+"\n")
        else: # link
            src = binmap[0][4:]
            fd.write("install-exec-local-%s: %s\n" % (name, src))
            fd.write("\t-mkdir -p $(DESTDIR)$(bindir)\n")
            fd.write("\t-$(RM) $(DESTDIR)$(bindir)/%s\n" % name)
            fd.write("\tcd $(DESTDIR)$(bindir); $(LN_S) %s %s\n\n" % (src, name))
            fd.write("uninstall-local-%s: \n" % name)
            fd.write("\t$(RM) $(DESTDIR)$(bindir)/%s\n\n" % name)
            am['INSTALL'].append(name)
            am['UNINSTALL'].append(name)
            am['InstallList'].append("\t$(bindir)/"+name+"\n")

            fd.write("all-local-%s: %s\n" % (name, src))
            fd.write("\t-$(RM) %s\n" % name)
            fd.write("\t$(LN_S) %s %s\n\n" % (src, name))
            am['ALL'].append(name)
        return

    SCRIPTS = []
    scripts_ext = []
    if binmap.has_key('SCRIPTS'):
        scripts_ext = binmap['SCRIPTS']

    name = var[4:]
    if binmap.has_key("NAME"):
        binname = binmap['NAME'][0]
    else:
        binname = name
    norm_binname = am_normalize(binname)

    bd = 'bindir'
    if binmap.has_key("DIR"):
        bd = binmap["DIR"][0] # use first name given
    bd = am_translate_dir(bd, am)
    fd.write("%sdir = %s\n" % (norm_binname, bd))

    cname = name
    cond = ''
    if binmap.has_key('COND'):
        condname = string.join(binmap['COND'], '+')
        cond = '#' + condname
        fd.write("if %s\n" % (condname))
        fd.write(" C_%s = %s\n" % (name,name))
        fd.write(" %s_PROGRAMS =%s\n" % (norm_binname,  binname))
        fd.write("endif\n")
        cname = "$(C_" + name + ")"
    else:
        fd.write("%s_PROGRAMS =%s\n" % (norm_binname,  binname))

    am['InstallList'].append("\t%s/%s%s\n" % (bd, binname, cond))

    if binmap.has_key('MTSAFE'):
        fd.write("CFLAGS %s $(THREAD_SAVE_FLAGS)\n" % am_assign)
        fd.write("CXXFLAGS %s $(THREAD_SAVE_FLAGS)\n" % am_assign)

    if binmap.has_key("LIBS"):
        fd.write(am_additional_libs(norm_binname, "", "BIN", binmap["LIBS"], am))

    if binmap.has_key("LDFLAGS"):
        fd.write(am_additional_flags(norm_binname, "", "BIN", binmap["LDFLAGS"], am))

    for src in binmap['SOURCES']:
        base, ext = split_filename(src)
        if ext not in automake_ext:
            am['EXTRA_DIST'].append(src)

    nsrcs = "nodist_"+norm_binname+"_SOURCES ="
    srcs = "dist_"+norm_binname+"_SOURCES ="
    for target in binmap['TARGETS']:
        t, ext = split_filename(target)
        if ext in scripts_ext:
            if target not in SCRIPTS:
                SCRIPTS.append(target)
        else:
            (dist,src) = am_find_srcs(target, binmap['DEPS'], am)
            if (dist):
                srcs = srcs + " " + src;
            else:
                nsrcs = nsrcs + " " + src;

    fd.write(nsrcs + "\n")
    fd.write(srcs + "\n")
    if len(SCRIPTS) > 0:
        fd.write("%s_scripts = %s\n" % (norm_binname, am_list2string(SCRIPTS, " ", "")))
        am['BUILT_SOURCES'].append("$(" + name + "_scripts)")
        fd.write("all-local-%s: $(%s_scripts)\n" % (name, name))
        am['ALL'].append(cname)

    am_find_hdrs(am, binmap)
    am_find_ins(am, binmap)

    am_deps(fd, binmap['DEPS'], ".o", am)

def am_bins(fd, var, binsmap, am):

    lbins = []
    scripts_ext = []
    if binsmap.has_key('SCRIPTS'):
        scripts_ext = binsmap['SCRIPTS']

    name = ""
    if binsmap.has_key("NAME"):
        name = binsmap["NAME"][0] # use first name given
    if binsmap.has_key('MTSAFE'):
        fd.write("CFLAGS %s $(THREAD_SAVE_FLAGS)\n" % am_assign)
        fd.write("CXXFLAGS %s $(THREAD_SAVE_FLAGS)\n" % am_assign)
    for binsrc in binsmap['SOURCES']:
        SCRIPTS = []
        bin, ext = split_filename(binsrc)
        if ext not in automake_ext:
            am['EXTRA_DIST'].append(binsrc)

        if binsmap.has_key("DIR"):
            lbins.append(bin)
        else:
            am['BINS'].append(bin)

        if binsmap.has_key(bin + "_LIBS"):
            fd.write(am_additional_libs(bin, "", "BIN", binsmap[bin + "_LIBS"], am))
        elif binsmap.has_key("LIBS"):
            fd.write(am_additional_libs(bin, "", "BIN", binsmap["LIBS"], am))

        if binsmap.has_key("LDFLAGS"):
            fd.write(am_additional_flags(bin, "", "BIN", binsmap["LDFLAGS"], am))

        nsrcs = "nodist_"+am_normalize(bin)+"_SOURCES ="
        srcs = "dist_"+am_normalize(bin)+"_SOURCES ="
        for target in binsmap['TARGETS']:
            t, ext = split_filename(target)
            if t == bin:
                t, ext = split_filename(target)
                if ext in scripts_ext:
                    if target not in SCRIPTS:
                        SCRIPTS.append(target)
                else:
                    (dist,src) = am_find_srcs(target, binsmap['DEPS'], am)
                    if dist:
                        srcs = srcs + " " + src
                    else:
                        nsrcs = nsrcs + " " + src
        fd.write(nsrcs + "\n")
        fd.write(srcs + "\n")

        if len(SCRIPTS) > 0:
            fd.write("%s_scripts = %s\n\n" % (name, am_list2string(SCRIPTS, " ", "")))
            am['BUILT_SOURCES'].append("$(" + name + "_scripts)")
            fd.write("all-local-%s: $(%s_scripts)\n" % (name, name))
            am['ALL'].append(name)

    if (len(lbins) > 0):
        bd = binsmap["DIR"][0] # use first name given
        bd = am_translate_dir(bd, am)
        fd.write("%sdir = %s\n" % (bin, bd))
        fd.write("%s_PROGRAMS =%s\n" % (bin,  am_list2string(lbins, " ", "") ))
        for bn in lbins:
            am['InstallList'].append("\t%s/%s\n" % (bd, bn))

    if binsmap.has_key('HEADERS'):
        HDRS = []
        hdrs_ext = binsmap['HEADERS']
        for target in binsmap['DEPS'].keys():
            t, ext = split_filename(target)
            if ext in hdrs_ext:
                am['HDRS'].append(target)

    am_find_ins(am, binsmap)
    am_deps(fd, binsmap['DEPS'], ".o", am)

def am_mods_to_libs(fd, var, modmap, am):
    modname = var[:-4]+"LIBS"
    am_assignment(fd, var, modmap, am)
    fd.write(am_additional_libs(modname, "_", "MOD", modmap, am))

def am_library(fd, var, libmap, am):
    name = var[4:]

    sep = ""
    pref = 'lib'
    if libmap.has_key("NAME"):
        libname = libmap['NAME'][0]
    else:
        libname = name

    if libmap.has_key("PREFIX"):
        if libmap['PREFIX']:
            pref = libmap['PREFIX'][0]
        else:
            pref = ''

    if libname[0] == "_":
        sep = "_"
        libname = libname[1:]
    if libmap.has_key('SEP'):
        sep = libmap['SEP'][0]

    cname = libname
    cond = ''
    condname = ''
    if libmap.has_key('COND'):
        condname = string.join(libmap['COND'], '+')
        cond = '#' + condname
        fd.write("if %s\n" % condname)
        fd.write(" C_%s = %s\n" % (libname, libname))
        cname = "$(C_" + libname + ")"

    if name[0] == '_':
        name = name[1:]
    fd.write("lib%s%s_la_CFLAGS=-DLIB%s $(AM_CFLAGS)\n" % (sep,libname,string.upper(name)))

    ld = "libdir"
    if libmap.has_key("DIR"):
        ld = libmap["DIR"][0] # use first name given

    SCRIPTS = []
    scripts_ext = []
    if libmap.has_key('SCRIPTS'):
        scripts_ext = libmap['SCRIPTS']

    ld = am_translate_dir(ld, am)
    fd.write("%sdir = %s\n" % (libname, ld))
    if libmap.has_key('NOINST'):
        am['NLIBS'].append((pref, libname, sep))
    else:
        am['LIBS'].append((pref, libname, sep, condname))
        am['InstallList'].append("\t%s/%s%s%s.so%s\n" % (ld, pref, sep, libname, cond))

    if libmap.has_key('MTSAFE'):
        fd.write("CFLAGS %s $(THREAD_SAVE_FLAGS)\n" % am_assign)
        fd.write("CXXFLAGS %s $(THREAD_SAVE_FLAGS)\n" % am_assign)

    if libmap.has_key("LIBS"):
        fd.write(am_additional_libs(libname, sep, "LIB", libmap["LIBS"], am, pref))
        fd.write(am_additional_install_libs(libname, sep, libmap["LIBS"], am))

    if libmap.has_key("LDFLAGS"):
        fd.write(am_additional_flags(libname, sep, "LIB", libmap["LDFLAGS"], am))

    for src in libmap['SOURCES']:
        base, ext = split_filename(src)
        if ext not in automake_ext:
            am['EXTRA_DIST'].append(src)

    nsrcs = "nodist_"+pref+sep+libname+"_la_SOURCES ="
    srcs = "dist_"+pref+sep+libname+"_la_SOURCES ="
    for target in libmap['TARGETS']:
        t, ext = split_filename(target)
        if ext in scripts_ext:
            if target not in SCRIPTS:
                SCRIPTS.append(target)
        else:
            (dist,src) = am_find_srcs(target, libmap['DEPS'], am)
            if dist:
                srcs = srcs + " " + src
            else:
                nsrcs = nsrcs + " " + src
    fd.write(nsrcs + "\n")
    fd.write(srcs + "\n")

    if cond:
        fd.write("endif\n")

    if len(SCRIPTS) > 0:
        fd.write("%s_scripts = %s\n" % (libname, am_list2string(SCRIPTS, " ", "")))
        am['BUILT_SOURCES'].append("$(" + libname + "_scripts)")
        fd.write("all-local-%s: $(%s_scripts)\n" % (libname, libname))
        am['ALL'].append(cname)

    am_find_hdrs(am, libmap)
    am_find_ins(am, libmap)

    am_deps(fd, libmap['DEPS'], ".lo", am)

def am_libs(fd, var, libsmap, am):

    ld = "libdir"
    if (libsmap.has_key("DIR")):
        ld = libsmap["DIR"][0] # use first name given

    sep = ""
    if libsmap.has_key('SEP'):
        sep = libsmap['SEP'][0]

    scripts_ext = []
    if libsmap.has_key('SCRIPTS'):
        scripts_ext = libsmap['SCRIPTS']

    if libsmap.has_key('MTSAFE'):
        fd.write("CFLAGS %s $(THREAD_SAVE_FLAGS)\n" % am_assign)
        fd.write("CXXFLAGS %s $(THREAD_SAVE_FLAGS)\n" % am_assign)

    libnames = []
    for libsrc in libsmap['SOURCES']:
        SCRIPTS = []
        libname, libext = split_filename(libsrc)
        if libext not in automake_ext:
            am['EXTRA_DIST'].append(libsrc)

        libnames.append(sep+libname)

# temporarily switched off, the by libtool created scripts cause problems
# for so-so linking
#    if libsmap.has_key(libname + "_LIBS"):
#      fd.write(am_additional_libs(libname, sep, "LIB", libsmap[libname + "_LIBS"], am))
#    elif libsmap.has_key("LIBS"):
#      fd.write(am_additional_libs(libname, sep, "LIB", libsmap["LIBS"], am))
        _libs = []
        if libsmap.has_key(libname + "_DLIBS"):
            _libs += libsmap[libname + "_DLIBS"]
            fd.write(am_additional_install_libs(libname, sep, libsmap[libname+ "_DLIBS"], am))

        if libsmap.has_key("LIBS"):
            _libs += libsmap["LIBS"];
        if libsmap.has_key("LDFLAGS"):
            _libs += libsmap["LDFLAGS"];
        if len(_libs) > 0:
            fd.write(am_additional_libs(libname, sep, "LIB", _libs, am))

        nsrcs = "nodist_"+"lib"+sep+libname+"_la_SOURCES ="
        srcs = "dist_"+"lib"+sep+libname+"_la_SOURCES ="
        for target in libsmap['TARGETS']:
            t, ext = split_filename(target)
            if t == libname:
                if ext in scripts_ext:
                    if target not in SCRIPTS:
                        SCRIPTS.append(target)
                else:
                    (dist,src) = am_find_srcs(target, libsmap['DEPS'], am)
                    if dist:
                        srcs = srcs + " " + src
                    else:
                        nsrcs = nsrcs + " " + src
        fd.write(nsrcs + "\n")
        fd.write(srcs + "\n")

        if len(SCRIPTS) > 0:
            fd.write("%s_scripts = %s\n\n" % (libname, am_list2string(SCRIPTS, " ", "")))
            am['BUILT_SOURCES'].append("$(" + libname + "_scripts)")
            fd.write("all-local-%s: $(%s_scripts)\n" % (libname, libname))
            am['ALL'].append(libname)

        ld = am_translate_dir(ld, am)
        fd.write("%sdir = %s\n" % (libname, ld))
        fd.write("lib%s%s_la_CFLAGS=-DLIB%s $(AM_CFLAGS)\n" % (sep,libname,string.upper(libname)))
        am['LIBS'].append(('lib', libname, sep, ''))
        am['InstallList'].append("\t"+ld+"/lib"+sep+libname+".so\n")

    if libsmap.has_key('HEADERS'):
        HDRS = []
        hdrs_ext = libsmap['HEADERS']
        for target in libsmap['DEPS'].keys():
            t, ext = split_filename(target)
            if ext in hdrs_ext:
                am['HDRS'].append(target)

    am_find_ins(am, libsmap)
    am_deps(fd, libsmap['DEPS'], ".lo", am)

def am_jar(fd, var, jar, am):

    name = var[4:]

    jd = "JARDIR"
    if jar.has_key("DIR"):
        jd = jar["DIR"][0] # use first name given
    jd = am_translate_dir(jd, am)

    for src in jar['SOURCES']:
        am['EXTRA_DIST'].append(src)

    fd.write("\nif HAVE_JAVA\n\n")

    if jar.has_key("MANIFEST") and len(jar['MANIFEST']) == 1:
        fd.write("%s_manifest_file= %s\n" % (name, am_translate_dir(jar['MANIFEST'][0],am)))
        manifest_flag='m'
    else:
        fd.write("%s_manifest_file= \n" % name)
        manifest_flag=''

    fd.write("%s_java_files= " % (name))
    for j in jar['SOURCES']:
        s,ext = rsplit_filename(j)
        if ext == 'in':
            fd.write('%s ' % s)
        else:
            fd.write('%s ' % j)

    fd.write("\n%s_class_files= " % (name))
    for j in jar['TARGETS']:
        # translate any \ path separators to / -- the generated file
        # is Unix/Linux/Cygwin only
        fd.write("%s " % j.replace('\\', '/'))

    fd.write("\n$(%s_class_files): $(%s_java_files)\n" % (name, name))
    fd.write("\tfor f in $(subst $$,\\$$,$^); do set $${1+\"$$@\"} \"`$(CYGPATH_W) $$f`\"; done; $(JAVAC) -d . -classpath \"`$(CYGPATH_WP) \"$(CLASSPATH)\"`\" $(JAVACFLAGS) $${1+\"$$@\"}\n")
    fd.write("%s.jar: $(%s_class_files) $(%s_manifest_file)\n" % (name, name, name))
    fd.write("\tfor f in $@ $(%s_manifest_file) $(subst $$,\\$$,$(%s_class_files)); do set $${1+\"$$@\"} \"`$(CYGPATH_W) $$f`\"; done; $(JAR) $(JARFLAGS) -cf%s $${1+\"$$@\"}\n" % (name, name, manifest_flag))
    fd.write("install-exec-local-%s_jar: %s.jar\n" % (name, name))
    fd.write("\t-mkdir -p $(DESTDIR)%s\n" % jd)
    fd.write("\t$(INSTALL) $< $(DESTDIR)%s/%s.jar\n" % (jd, name))

    fd.write("uninstall-local-%s_jar:\n" % name)
    fd.write("\t$(RM) $(DESTDIR)%s/%s.jar\n" % (jd, name))

    fd.write("all-local-%s_jar: %s.jar\n" % (name, name))
    am['ALL'].append(name+"_jar")

    fd.write("\nelse\n\n")

    fd.write("install-exec-local-%s_jar:\n" % name)
    fd.write("uninstall-local-%s_jar:\n" % name)
    fd.write("all-local-%s_jar:\n" % name)

    fd.write("\nendif !HAVE_JAVA\n\n")

    am['INSTALL'].append(name+"_jar")
    am['UNINSTALL'].append(name+"_jar")
    am['InstallList'].append("\t"+jd+"/"+name+".jar\n")

    am_find_ins(am, jar)

def am_java(fd, var, java, am):

    name = var[5:]

    jd = "JAVADIR"
    if java.has_key("DIR"):
        jd = java["DIR"][0] # use first name given
    jd = am_translate_dir(jd, am)

    for src in java['SOURCES']:
        am['EXTRA_DIST'].append(src)

    fd.write("\nif HAVE_JAVA\n\n")

    fd.write("%s_java_files= %s\n" % (name, am_list2string(java['SOURCES'], " ", "")))
    fd.write("\n%s_class_files= " % (name))
    for j in java['TARGETS']:
        fd.write("%s " % j)

    fd.write("\n$(%s_class_files): $(%s_java_files)\n" % (name, name))
    fd.write("\tfor f in $(subst $$,\\$$,$^); do set $${1+\"$$@\"} \"`$(CYGPATH_W) $$f`\"; done; $(JAVAC) -d . -classpath \"`$(CYGPATH_WP) \"$(CLASSPATH)\"`\" $(JAVACFLAGS) $${1+\"$$@\"}\n")

    fd.write("install-exec-local-%s_class: %s.class\n" % (name, name))
    fd.write("\t-mkdir -p $(DESTDIR)%s\n" % jd)
    fd.write("\t$(INSTALL) $< $(DESTDIR)%s/%s.class\n" % (jd, name))

    fd.write("uninstall-local-%s_class:\n" % name)
    fd.write("\t$(RM) $(DESTDIR)%s/%s.class\n" % (jd, name))

    fd.write("all-local-%s_class: %s.class\n" % (name, name))
    am['ALL'].append(name+"_class")

    fd.write("\nelse\n\n")

    fd.write("install-exec-local-%s_class:\n" % name)
    fd.write("uninstall-local-%s_class:\n" % name)
    fd.write("all-local-%s_class:\n" % name)

    fd.write("\nendif !HAVE_JAVA\n\n")

    am['INSTALL'].append(name+"_class")
    am['UNINSTALL'].append(name+"_class")
    am['InstallList'].append("\t"+jd+"/"+name+".class\n")

    am_find_ins(am, java)

def am_add_srcdir(path, am, prefix =""):
    dir = path
    if dir[0] == '$':
        return ""
    elif not os.path.isabs(dir):
        dir = "$(srcdir)/" + dir
    else:
        return ""
    return prefix+dir

def am_translate_dir(path, am):
    # translate any \ path separators to / -- the generated file is
    # Unix/Linux/Cygwin only
    path = path.replace('\\', '/')
    dir = path
    rest = ""
    if string.find(path, '/') >= 0:
        dir, rest = string.split(path, '/', 1)
        rest = '/' + rest

    if dir in ('bindir', 'builddir', 'datadir', 'includedir', 'infodir',
               'libdir', 'libexecdir', 'localstatedir', 'mandir',
               'oldincludedir', 'pkgbindir', 'pkgdatadir', 'pkgincludedir',
               'pkglibdir', 'pkglocalstatedir', 'pkgsysconfdir', 'sbindir',
               'sharedstatedir', 'srcdir', 'sysconfdir', 'top_builddir',
               'top_srcdir'):
        dir = "$("+dir+")"
    dir = dir + rest
    return dir

def am_includes(fd, var, values, am):
    incs = "-I$(srcdir)"
    for i in values:
        if i[0] == "-" or i[0] == "$":
            incs = incs + " " + i
        else:
            incs = incs + " -I" + am_translate_dir(i, am) \
                   + am_add_srcdir(i, am, " -I")
    fd.write("INCLUDES = " + incs + "\n")

output_funcs = {'SUBDIRS': am_subdirs,
                'EXTRA_DIST': am_extra_dist,
                'EXTRA_DIST_DIR': am_extra_dist_dir,
                'EXTRA_HEADERS': am_extra_headers,
                'LIBDIR': am_libdir,
                'LIBS': am_libs,
                'LIB': am_library,
                'BINS': am_bins,
                'BIN': am_binary,
                'DOC': am_doc,
                'INCLUDES': am_includes,
                'MTSAFE': am_mtsafe,
                'SCRIPTS': am_scripts,
                'CFLAGS': am_cflags,
                'CXXFLAGS': am_cflags,
                'STATIC_MODS': am_mods_to_libs,
                'smallTOC_SHARED_MODS': am_mods_to_libs,
                'largeTOC_SHARED_MODS': am_mods_to_libs,
                'HEADERS': am_headers,
                'JAR': am_jar,
                'JAVA': am_java,
                }

def output(tree, cwd, topdir, automake, conditional):
    global am_assign
    #if int(automake) >= 1005000 and int(automake) < 1006000:
    #    am_assign = "="

    # use binary mode since automake on Cygwin can't deal with \r\n
    # line endings
    fd = open(os.path.join(cwd, 'Makefile.am'), "wb")

    fd.write('''
## This file is generated by autogen.py, do not edit
## Process this file with automake to produce Makefile.in
## autogen includes dependencies so automake doesn\'t need to generated them

AUTOMAKE_OPTIONS = no-dependencies 1.4 foreign

CXXEXT = \\\"cc\\\"

''')

    if not tree.has_key('INCLUDES'):
        tree.add('INCLUDES', [])

    am = {}
    if tree.has_key('NAME'):
        am['NAME'] = tree['NAME']
    else:
        if cwd != topdir:
            am['NAME'] = os.path.basename(cwd)
        else:
            am['NAME'] = ''

    name = am['NAME']
    am['TOPDIR'] = topdir
    am['CWD'] = ''
    if cwd != topdir:
        # in case we happen to be running this on Windows, replace dir seps
        am['CWD'] = cwd[len(topdir)+1:].replace('\\', '/')+'/'
    am['BUILT_SOURCES'] = []
    am['EXTRA_DIST'] = []
    am['LIBS'] = []     # all libraries (am_libs and am_library)
    am['NLIBS'] = []     # all libraries which are not installed
    am['BINS'] = []
    am['BIN_SCRIPTS'] = []
    am['INSTALL'] = []
    am['DATA_INSTALL'] = []
    am['UNINSTALL'] = []
    am['HDRS'] = []
    am['LIBDIR'] = "libdir"
    am['ALL'] = []
    am['DEPS'] = []
    if conditional:
        cond = '#' + string.join(conditional, '+')
    else:
        cond = ''
    am['InstallList'] = []
    am['InstallList'].append(am['CWD']+cond+"\n")
    am['DocList'] = []
    am['DocList'].append(am['CWD']+cond+"\n")
    am['OutList'] = [am['CWD'] + 'Makefile']

    for i, v in tree.items():
        j = i
        if string.find(i, '_') >= 0:
            k, j = string.split(i, '_', 1)
            j = string.upper(k)
        if output_funcs.has_key(i):
            output_funcs[i](fd, i, v, am)
        elif output_funcs.has_key(j):
            output_funcs[j](fd, i, v, am)
        elif i != 'TARGETS':
            am_assignment(fd, i, v, am)

    if len(am['BUILT_SOURCES']) > 0:
        fd.write("BUILT_SOURCES =%s\n" % am_list2string(am['BUILT_SOURCES'], " ", ""))
        # the BUILT_SOURCES should be cleaned up by make (mostly)clean
        fd.write("MOSTLYCLEANFILES =%s\n" % am_list2string(am['BUILT_SOURCES'], " ", ""))

    fd.write("EXTRA_DIST = Makefile.ag Makefile.msc%s\n" % \
          am_list2string(am['EXTRA_DIST'], " ", ""))

    if am['LIBS']:
        lib = 'lib'
        ld = am['LIBDIR']
        ld = am_translate_dir(ld, am)
        if ld != '$(libdir)':
            fd.write("agdir = %s\n" % ld)
            lib = 'ag'

        libs = am_sort_libs(am['LIBS'], tree)
        s = ""
        for (pref, lib, sep, cond) in am['LIBS']:
            if cond != '':
                fd.write("if %s\n" % (cond))
                fd.write("%s_LTLIBRARIES = %s%s%s.la\n" % (lib, pref, sep, lib))
                fd.write("endif\n")
            else:
                fd.write("%s_LTLIBRARIES = %s%s%s.la\n" % (lib, pref, sep, lib))

    if am['NLIBS']:
        fd.write("noinst_LTLIBRARIES =")
        for (pref, lib, sep) in am['NLIBS']:
            fd.write(" %s%s%s.la" % (pref, sep, lib))
        fd.write("\n")

    if len(am['BINS']) > 0:
        fd.write("bin_PROGRAMS =%s\n" % am_list2string(am['BINS'], " ", ""))
        for i in am['BINS']:
            am['InstallList'].append("\t$(bindir)/"+i+"\n")

    if len(am['BIN_SCRIPTS']) > 0:
        scripts = am['BIN_SCRIPTS']
        fd.write("bin_SCRIPTS = %s\n" % am_list2string(scripts, " ", ""))
        fd.write("install-exec-local-SCRIPTS: \n")
        fd.write("all-local-SCRIPTS: $(bin_SCRIPTS)\n")

    if len(am['UNINSTALL']) > 0:
        fd.write("uninstall-local:%s\n" % \
            am_list2string(am['UNINSTALL'], " uninstall-local-", ""))

    if len(am['INSTALL']) > 0:
        fd.write("install-exec-local:%s\n" % \
            am_list2string(am['INSTALL'], " install-exec-local-", ""))

    if len(am['DATA_INSTALL']) > 0:
        fd.write("install-data-local:%s\n" % \
            am_list2string(am['DATA_INSTALL'], " install-data-local-", ""))

    if len(am['ALL']) > 0:
        fd.write("all-local:%s\n" % \
            am_list2string(am['ALL'], " all-local-", ""))

    if len(am['HDRS']) > 0:
        incs = ""
        # breaks make dist, used to be needed for sub projects?
        #if os.path.exists(".incs.in"):
            #incs = ".incs.in"
        if len(name) > 0:
            fd.write("%sincludedir = $(pkgincludedir)/%s\n" % (name, name))
        else:
            name="top"
            fd.write("%sincludedir = $(pkgincludedir)\n" % (name))
        fd.write("nodist_%sinclude_HEADERS = %s %s\n" % (name, am_list2string(am['HDRS'], " ", ""), incs))

    fd.write('''
include $(top_srcdir)/*.mk
include $(top_builddir)/*.mk
''')
    fd.close()

    return am['InstallList'], am['DocList'], am['OutList']
