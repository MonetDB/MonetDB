import string
import os
import regsub

#automake_ext = [ 'c', 'cc', 'h', 'y', 'yy', 'l', 'll', 'glue.c' ]
automake_ext = [ 'c', 'cc', 'h', 'tab.c', 'tab.cc', 'tab.h', 'yy.c', 'yy.cc', 'glue.c', 'proto.h' ]
script_ext = [ 'mil' ]

def am_assignment(fd, var, values, am ):
  o = ""
  for v in values:
    o = o + " " + v
  fd.write("%s = %s\n" % (var,o) )

def am_cflags(fd, var, values, am ):
  o = ""
  for v in values:
    o = o + " " + v
  fd.write("%s += %s\n" % (var,o) )

def am_extra_dist(fd, var, values, am ):
  for i in values:
    am['EXTRA_DIST'].append(i)

def am_extra_dist_dir(fd, var, values, am ):
  fd.write("dist-hook:\n")
  for i in values:
    fd.write("\tmkdir $(distdir)/%s\n" % (i))
    fd.write("\tcp -R $(srcdir)/%s/[^C]* $(distdir)/%s\n" % (i,i))

def am_extra_headers(fd, var, values, am ):
  for i in values:
    am['HDRS'].append(i)

def am_libdir(fd, var, values, am ):
  am['LIBDIR'] = values[0]

def am_mtsafe(fd, var, values, am ):
  fd.write("CFLAGS+=$(thread_safe_flag_spec)\n")
  fd.write("CXXFLAGS+=$(thread_safe_flag_spec)\n")

def am_list2string(l,pre,post):
  res = ""
  for i in l:
    res = res + pre + i + post
  return res

def am_find_srcs(target,deps,am):
  base,ext = string.split(target,".", 1) 	
  f = target
  pf = f
  while (ext != "h" and deps.has_key(f) ):
    f = deps[f][0]
    b,ext = string.split(f,".",1)
    if (ext in automake_ext):
      pf = f 
  # built source if has dep and ext != cur ext
  if (deps.has_key(pf) and pf not in am['BUILT_SOURCES']):
	pfb,pfext = string.split(pf,".",1)
	sfb,sfext = string.split(deps[pf][0],".",1)
	if (sfext != pfext):
		am['BUILT_SOURCES'].append(pf)
  return pf

def am_find_hdrs(target,deps,hdrs):
  base,ext = string.split(target,".", 1) 	
  f = target
  pf = f
  while (ext != "h" and deps.has_key(f) ):
    f = deps[f][0]
    b,ext = string.split(f,".",1)
    if (ext in automake_ext):
      pf = f 
  return pf

def am_additional_libs(name,sep,type,list, am):
    if (type == "BIN"):
    	add = name+"_LDADD =" 
    else:
    	add = "lib"+sep+name+"_la_LIBADD =" 
    for l in list:
      if (l[0] == "-" or l[0] == "$"):
      	add = add + " " + l 
      else:
      	add = add + " " + am_translate_dir(l,am) + ".la"
    return add + "\n"
  
def am_deps(fd,deps,objext, am):
  for t,deplist in deps.items():
    t = regsub.sub("\.o",objext,t)
    fd.write( t + ":" )
    for d in deplist:
      fd.write( " " + am_translate_dir(d,am) )
    fd.write("\n");


def am_binary(fd, var, binmap, am ):
  
  if (type(binmap) == type([])):
    name = var[4:]
    if (name == 'SCRIPTS'):
      fd.write("bin_SCRIPTS = %s\n" % am_list2string(binmap," ",""))
      fd.write("install-exec-local-SCRIPTS: \n" )
      fd.write("all-local-SCRIPTS: $(bin_SCRIPTS)\n" )
      fd.write("%s" % am_list2string(binmap,"\tchmod a+x ","\n"))
      am['INSTALL'].append(name)
    else: # link
      src = binmap[0][4:]
      fd.write("install-exec-local-%s: %s\n" % (name,src))
      fd.write("\t$(RM) $(DESTDIR)$(bindir)/%s\n" % (name))
      fd.write("\tcd $(DESTDIR)$(bindir); $(LN_S) %s %s\n\n" % (src,name))
      fd.write("uninstall-exec-local-%s: \n" % (name))
      fd.write("\t$(RM) $(DESTDIR)$(bindir)/%s\n\n" % (name))

      fd.write("all-local-%s: %s\n" % (name,src))
      fd.write("\t$(RM) %s\n" % (name))
      fd.write("\t$(LN_S) %s %s\n\n" % (src,name))
      am['INSTALL'].append(name)
    return

  HDRS = []
  hdrs_ext = []
  if (binmap.has_key('HEADERS')):
	hdrs_ext = binmap['HEADERS']

  SCRIPTS = []
  scripts_ext = []
  if (binmap.has_key('SCRIPTS')):
	scripts_ext = binmap['SCRIPTS']

  name = var[4:]
  if (binmap.has_key("NAME")):
    binname = binmap['NAME'][0]
  else:
    binname = name
  am['BINS'].append(binname)
  if (binmap.has_key('MTSAFE')):
    fd.write("CFLAGS+=$(thread_safe_flag_spec)\n")
    fd.write("CXXFLAGS+=$(thread_safe_flag_spec)\n")

  if (binmap.has_key("LIBS")):
    fd.write(am_additional_libs(binname, "", "BIN", binmap["LIBS"],am))

  for src in binmap['SOURCES']:
    base,ext = string.split(src,".", 1) 	
    if (ext not in automake_ext):
      am['EXTRA_DIST'].append(src)
	
  srcs = binname+"_SOURCES ="
  for target in binmap['TARGETS']:
    t,ext = string.split(target,".",1)
    if (ext in hdrs_ext):
      HDRS.append(target)
    if (ext in scripts_ext):
      if (target not in SCRIPTS):
        SCRIPTS.append(target)
    else:
      srcs = srcs + " " + am_find_srcs(target,binmap['DEPS'], am) 
  fd.write(srcs + "\n")
  if (len(SCRIPTS) > 0):
    fd.write("%s_scripts = %s\n" % (binname,am_list2string(SCRIPTS," ","")))
    am['BUILT_SOURCES'].append("$(" + name + "_scripts)")

  if (binmap.has_key('HEADERS')):
    for h in HDRS:
	am['HDRS'].append(h)

  am_deps(fd,binmap['DEPS'],".o",am);

def am_bins(fd, var, binsmap, am ):

  HDRS = []
  hdrs_ext = []
  if (binsmap.has_key('HEADERS')):
	hdrs_ext = binsmap['HEADERS']

  scripts_ext = []
  if (binsmap.has_key('SCRIPTS')):
	scripts_ext = binsmap['SCRIPTS']

  name = ""
  if (binsmap.has_key("NAME")):
    name = binsmap["NAME"][0] # use first name given
  if (binsmap.has_key('MTSAFE')):
    fd.write("CFLAGS+=$(thread_safe_flag_spec)\n")
    fd.write("CXXFLAGS+=$(thread_safe_flag_spec)\n")
  for binsrc in binsmap['SOURCES']:
    SCRIPTS = []
    bin,ext = string.split(binsrc,".", 1) 	
    if (ext not in automake_ext):
      am['EXTRA_DIST'].append(binsrc)
    am['BINS'].append(bin)
	
    if (binsmap.has_key(bin + "_LIBS")):
      fd.write(am_additional_libs(bin, "", "BIN", binsmap[bin + "_LIBS"],am))
    elif (binsmap.has_key("LIBS")):
      fd.write(am_additional_libs(bin, "", "BIN", binsmap["LIBS"],am))

    srcs = bin+"_SOURCES ="
    for target in binsmap['TARGETS']:
      l = len(bin)
      if (target[0:l] == bin):
        t,ext = string.split(target,".",1)
	if (ext in hdrs_ext):
	  HDRS.append(target)
        if (ext in scripts_ext):
          if (target not in SCRIPTS):
            SCRIPTS.append(target)
        else:
          srcs = srcs + " " + am_find_srcs(target,binsmap['DEPS'], am) 
    fd.write(srcs + "\n")
    if (len(SCRIPTS) > 0):
      fd.write("%s_scripts = %s\n\n" % (name,am_list2string(SCRIPTS," ","")))
      am['BUILT_SOURCES'].append("$(" + name + "_scripts)")

  if (binsmap.has_key('HEADERS')):
    for h in HDRS:
	am['HDRS'].append(h)

  am_deps(fd,binsmap['DEPS'],".o",am);

def am_library(fd, var, libmap, am ):

  HDRS = []
  hdrs_ext = []
  if (libmap.has_key('HEADERS')):
    hdrs_ext = libmap['HEADERS']

  SCRIPTS = []
  scripts_ext = []
  if (libmap.has_key('SCRIPTS')):
    scripts_ext = libmap['SCRIPTS']

  name = var[4:]
  sep = ""
  if (libmap.has_key("NAME")):
    libname = libmap['NAME'][0]
  else:
    libname = name

  if (libname[0] == "_"):
     sep = "_"
     libname = libname[1:]

  am['LIBS'].append(sep+libname)
  if (libmap.has_key('MTSAFE')):
    fd.write("CFLAGS+=$(thread_safe_flag_spec)\n")
    fd.write("CXXFLAGS+=$(thread_safe_flag_spec)\n")

# temporarily switched off, the by libtool created scripts cause problems
# for so-so linking
#  if (libmap.has_key("LIBS")):
#    fd.write(am_additional_libs(libname, sep, "LIB", libmap["LIBS"],am))

  for src in libmap['SOURCES']:
    base,ext = string.split(src,".", 1) 	
    if (ext not in automake_ext):
      am['EXTRA_DIST'].append(src)
	
  srcs = "lib"+sep+libname+"_la_SOURCES ="
  for target in libmap['TARGETS']:
    t,ext = string.split(target,".",1)
    if (ext in hdrs_ext):
      HDRS.append(target)
    if (ext in scripts_ext):
      if (target not in SCRIPTS):
        SCRIPTS.append(target)
    else:
      srcs = srcs + " " + am_find_srcs(target,libmap['DEPS'], am) 
  fd.write(srcs + "\n")
  if (len(SCRIPTS) > 0):
    fd.write("%s_scripts = %s\n" % (libname,am_list2string(SCRIPTS," ","")))
    am['BUILT_SOURCES'].append("$(" + libname + "_scripts)")

  if (libmap.has_key('HEADERS')):
    for h in HDRS:
	am['HDRS'].append(h)

  am_deps(fd,libmap['DEPS'],".lo",am)

def am_libs(fd, var, values, am ):

  if (values.has_key('SEP')):
  	sep = values['SEP'][0]

  scripts_ext = []
  if (values.has_key('SCRIPTS')):
    scripts_ext = values['SCRIPTS']

  if (values.has_key('MTSAFE')):
    fd.write("CFLAGS+=$(thread_safe_flag_spec)\n")
    fd.write("CXXFLAGS+=$(thread_safe_flag_spec)\n")

  for libsrc in values['SOURCES']:
    SCRIPTS = []
    lib,libext = string.split(libsrc,".", 1) 	
    if (libext not in automake_ext):
      am['EXTRA_DIST'].append(libsrc)
    am['LIBS'].append(sep+lib)
	
# temporarily switched off, the by libtool created scripts cause problems
# for so-so linking
#    if (values.has_key(lib + "_LIBS")):
#      fd.write(am_additional_libs(lib, sep, "LIB", values[lib + "_LIBS"],am))
#    elif (values.has_key("LIBS")):
#      fd.write(am_additional_libs(lib, sep, "LIB", values["LIBS"],am))
    if (values.has_key(lib + "_DLIBS")):
      print(values[lib+"_DLIBS"])
      fd.write(am_additional_libs(lib, sep, "LIB", values[lib + "_DLIBS"],am))

    srcs = "lib"+sep+lib+"_la_SOURCES ="
    for target in values['TARGETS']:
      l = len(lib)
      if (target[0:l] == lib):
        t,ext = string.split(target,".",1)
        if (ext in scripts_ext):
          if (target not in SCRIPTS):
            SCRIPTS.append(target)
        else:
          srcs = srcs + " " + am_find_srcs(target,values['DEPS'], am) 
    fd.write(srcs + "\n")
    if (len(SCRIPTS) > 0):
      fd.write("%s_scripts = %s\n\n" % (lib,am_list2string(SCRIPTS," ","")))
      am['BUILT_SOURCES'].append("$(" + lib + "_scripts)")

  if (values.has_key('HEADERS')):
    HDRS = []
    hdrs_ext = values['HEADERS']
    for target in values['DEPS'].keys():
      t,ext = string.split(target,".",1)
      if (ext in hdrs_ext):
        am['HDRS'].append(target)

  am_deps(fd,values['DEPS'],".lo",am)

def am_translate_dir(path,am):
    dir = path
    if (string.find(path,os.sep) >= 0):
      d,rest = string.split(path,os.sep, 1) 
      if (d == "top_srcdir" or d == "top_builddir" or \
	  d == "srcdir" or d == "builddir"):
	dir = "$("+d+")"+ os.sep + rest
    return dir

def am_includes(fd, var, values, am):
  incs = ""
  for i in values:
    if (i[0] == "-" or i[0] == "$"):
      	incs = incs + " " + i
    else:
	incs = incs + " -I" + am_translate_dir(i,am)
  fd.write("INCLUDES = " + incs + "\n")

output_funcs = { 'SUBDIRS': am_assignment, 
	 	 'EXTRA_DIST': am_extra_dist,
	 	 'EXTRA_DIST_DIR': am_extra_dist_dir,
	 	 'EXTRA_HEADERS': am_extra_headers,
 		 'LIBDIR': am_libdir,
		 'LIBS' : am_libs,
		 'LIB' : am_library,
		 'BINS' : am_bins,
		 'BIN' : am_binary,
 		 'INCLUDES' : am_includes,
		 'MTSAFE' : am_mtsafe,
		 'CFLAGS' : am_cflags,
		 'CXXFLAGS' : am_cflags,
		}


# TODO
# make a class am
#
def output(tree, cwd, topdir):
  fd = open(cwd+os.sep+'Makefile.am',"w")

  fd.write('''
## This file is generated by autogen.py, do not edit
## Process this file with automake to produce Makefile.in
## autogen includes dependencies so automake doesn't need to generated them

AUTOMAKE_OPTIONS = no-dependencies 1.4 foreign

CXXEXT = \\\"cc\\\"

''')

  am = {}
  if ('NAME' in tree.keys()):
  	am['NAME'] = tree.value('NAME')
  else:
	if (cwd != topdir):
	  path = cwd
    	  if (string.find(path,os.sep) >= 0):
		l = string.split(path,os.sep)
  		am['NAME'] = l[len(l)-1]
	  else:
  		am['NAME'] = path
	else:
  	  am['NAME'] = ''
	
  name = am['NAME']
  am['TOPDIR'] = topdir
  am['BUILT_SOURCES'] = []
  am['EXTRA_DIST'] = []
  am['LIBS'] = []
  am['BINS'] = []
  am['INSTALL'] = []
  am['HDRS'] = []
  am['LIBDIR'] = "lib"
  for i in tree.keys():
    j = string.upper(i[0:3])
    if (output_funcs.has_key(i)):
	output_funcs[i](fd,i,tree.value(i),am)
    elif (output_funcs.has_key(j)):
	output_funcs[j](fd,i,tree.value(i),am)
    elif( i != 'TARGETS'):
	am_assignment(fd,i,tree.value(i),am)

  if (len(am['BUILT_SOURCES']) > 0):
   fd.write("BUILT_SOURCES =%s\n" % am_list2string(am['BUILT_SOURCES']," ",""))

  fd.write("EXTRA_DIST = Makefile.ag Makefile.msc%s\n" % \
	am_list2string(am['EXTRA_DIST']," ",""))

  if (len(am['LIBS']) > 0):
    fd.write("%s_LTLIBRARIES =%s\n" % \
		(am['LIBDIR'], am_list2string(am['LIBS']," lib",".la")))

  if (len(am['BINS']) > 0):
    fd.write("bin_PROGRAMS =%s\n" % am_list2string(am['BINS']," ",""))

  if (len(am['INSTALL']) > 0):
    fd.write("install-exec-local:%s\n" % \
	am_list2string(am['INSTALL']," install-exec-local-",""))
    fd.write("all-local:%s\n" % \
	am_list2string(am['INSTALL']," all-local-",""))

  if (len(am['HDRS']) > 0):
    if (len(name) > 0): 
      fd.write("%sincludedir = $(includedir)/%s\n" % (name,name))
    fd.write("%sinclude_HEADERS = %s\n" % (name,am_list2string(am['HDRS']," ","")))

  fd.write('''
include $(top_srcdir)/rules.mk

''')
  fd.close();
