import string
import regsub
import os

automake_ext = [ 'c', 'cc', 'h', 'y', 'yy', 'l', 'll', 'glue.c' ]
script_ext = [ 'mil' ]

def msc_subdirs(fd, var, values, msc ):
  o = ""
  for v in values:
    o = o + " " + v
  fd.write("%s = %s\n" % (var,o) )
  fd.write("all-recursive: $(SUBDIRS)\n")
  for v in values:
    fd.write("\t$(CD) %s && $(MAKE) /f Makefile.msc all\n" % v) 
  fd.write("install-recursive: $(SUBDIRS)\n")
  for v in values:
    fd.write("\t$(CD) %s && $(MAKE) /f Makefile.msc install\n" % v) 

def msc_assignment(fd, var, values, msc ):
  o = ""
  for v in values:
    o = o + " " + v
  fd.write("%s = %s\n" % (var,o) )

def msc_cflags(fd, var, values, msc ):
  o = ""
  for v in values:
    o = o + " " + v
  fd.write("CFLAGS = $(CFLAGS) %s\n" % (o) )

def msc_extra_dist(fd, var, values, msc ):
  for i in values:
    msc['EXTRA_DIST'].append(i)

def msc_extra_headers(fd, var, values, msc ):
  for i in values:
    msc['HDRS'].append(i)

def msc_libdir(fd, var, values, msc ):
  msc['LIBDIR'] = values[0]

def msc_mtsafe(fd, var, values, msc ):
  fd.write("CFLAGS=$(CFLAGS) $(thread_safe_flag_spec)\n")

def msc_translate_dir(path,msc):
    dir = path
    if (string.find(path,os.sep) >= 0):
      d,rest = string.split(path,os.sep, 1) 
      if (d == "top_srcdir" or d == "top_builddir"):
	  d = "$(TOPDIR)"
      elif (d == "srcdir" or d == "builddir"):
          d = "."
      dir = d+ "\\" + rest
    return regsub.gsub(os.sep, "\\", dir );

def msc_list2string(l,pre,post):
  res = ""
  for i in l:
    res = res + pre + i + post
  return res

def msc_space_sep_list(l):
  res = ""
  for i in l:
    res = res + " " + i
  return res + "\n"

def msc_find_srcs(target,deps,msc):
  base,ext = string.split(target,".", 1) 	
  f = target
  pf = f
  while (ext != "h" and deps.has_key(f) ):
    f = deps[f][0]
    b,ext = string.split(f,".",1)
    if (ext in automake_ext):
      pf = f 
  # built source if has dep and ext != cur ext
  if (deps.has_key(pf) and pf not in msc['BUILT_SOURCES']):
	pfb,pfext = string.split(pf,".",1)
	sfb,sfext = string.split(deps[pf][0],".",1)
	if (sfext != pfext):
		msc['BUILT_SOURCES'].append(pf)
  return pf

def msc_find_hdrs(target,deps,hdrs):
  base,ext = string.split(target,".", 1) 	
  f = target
  pf = f
  while (ext != "h" and deps.has_key(f) ):
    f = deps[f][0]
    b,ext = string.split(f,".",1)
    if (ext in automake_ext):
      pf = f 
  return pf

def msc_additional_libs(name,sep,type,list, msc):
    if (type == "BIN"):
    	add = name+"_LIBS =" 
    else:
    	add = "lib"+sep+name+"_LIBS =" 
    for l in list:
      if (l[0] == "-" or l[0] == "$"):
      	add = add + " " + l 
      else:
      	add = add + " " + msc_translate_dir(l,msc) + ".lib"
    return add + "\n"
  
def msc_translate_ext( f ):
	n = regsub.gsub("\.o", ".obj", f );
	return regsub.gsub("\.cc", ".cxx", n );

def msc_deps(fd,deps,objext, msc):
  for t,deplist in deps.items():
    t = msc_translate_ext(t)
    b,ext = string.split(t,".",1)
    fd.write( t + ":" )
    for d in deplist:
      fd.write( " " + msc_translate_dir(msc_translate_ext(d),msc) )
    fd.write("\n");
    if (ext == "glue.h"):
	fd.write( "\t$(MEL) $(INCLUDES) -o $@ -glue $<\n" );
    if (ext == "proto.h"):
	fd.write( "\t$(MEL) $(INCLUDES) -o $@ -proto $<\n" );

def msc_binary(fd, var, binmap, msc ):

  if (type(binmap) == type([])):
    name = var[4:]
    if (name == 'SCRIPTS'):
      for i in binmap:
        msc['INSTALL'].append((i,i))
    else: # link
      src = binmap[0][4:]
      msc['INSTALL'].append((name,src))
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
  msc['BINS'].append(binname)
  if (binmap.has_key('MTSAFE')):
    fd.write("CFLAGS=$(CFLAGS) $(thread_safe_flag_spec)\n")

  if (binmap.has_key("LIBS")):
    fd.write(msc_additional_libs(binname, "", "BIN", binmap["LIBS"],msc))
	
  srcs = binname+"_OBJS ="
  for target in binmap['TARGETS']:
    t,ext = string.split(target,".",1)
    if (ext == "o"):
      srcs = srcs + " " + t + ".obj" 
    elif (ext in hdrs_ext):
      HDRS.append(target)
    elif (ext in scripts_ext):
      if (target not in SCRIPTS):
        SCRIPTS.append(target)
  fd.write(srcs + "\n")
  fd.write( "%s.exe: $(%s_OBJS)\n" % (binname,binname))
  fd.write("\t$(CC) $(CFLAGS) -Fe%s.exe $(%s_OBJS) $(%s_LIBS) $(LDFLAGS) /subsystem:console\n\n" % (binname,binname,binname)) 

  if (len(SCRIPTS) > 0):
    fd.write(binname+"_SCRIPTS =" + msc_space_sep_list(SCRIPTS))
    msc['BUILT_SOURCES'].append("$(" + binname + "_SCRIPTS)")

  if (binmap.has_key('HEADERS')):
    for h in HDRS:
	msc['HDRS'].append(h)

  msc_deps(fd,binmap['DEPS'],".obj",msc);

def msc_bins(fd, var, binsmap, msc ):

  HDRS = []
  hdrs_ext = []
  if (binsmap.has_key('HEADERS')):
	hdrs_ext = binsmap['HEADERS']

  SCRIPTS = []
  scripts_ext = []
  if (binsmap.has_key('SCRIPTS')):
	scripts_ext = binsmap['SCRIPTS']

  name = ""
  if (binsmap.has_key("NAME")):
    name = binsmap["NAME"][0] # use first name given
  if (binsmap.has_key('MTSAFE')):
    fd.write("CFLAGS=$(CFLAGS) $(thread_safe_flag_spec)\n")
  for binsrc in binsmap['SOURCES']:
    bin,ext = string.split(binsrc,".", 1) 	
    if (ext not in automake_ext):
      msc['EXTRA_DIST'].append(binsrc)
    msc['BINS'].append(bin)
	
    if (binsmap.has_key(bin + "_LIBS")):
      fd.write(msc_additional_libs(bin, "", "BIN", binsmap[bin + "_LIBS"],msc))
    elif (binsmap.has_key("LIBS")):
      fd.write(msc_additional_libs(bin, "", "BIN", binsmap["LIBS"],msc))

    srcs = bin+"_OBJS ="
    for target in binsmap['TARGETS']:
      l = len(bin)
      if (target[0:l] == bin):
        t,ext = string.split(target,".",1)
        if (ext == "o"):
          srcs = srcs + " " + t + ".obj" 
	elif (ext in hdrs_ext):
	  HDRS.append(target)
        elif (ext in scripts_ext):
          if (target not in SCRIPTS):
            SCRIPTS.append(target)
    fd.write(srcs + "\n")
    fd.write( "%s.exe: $(%s_OBJS)\n" % (bin,bin))
    fd.write("\t$(CC) $(CFLAGS) -Fe%s.exe $(%s_OBJS) $(%s_LIBS) $(LDFLAGS) /subsystem:console\n\n" % (bin,bin,bin)) 

  if (len(SCRIPTS) > 0):
    fd.write(name+"_SCRIPTS =" + msc_space_sep_list(SCRIPTS))
    msc['BUILT_SOURCES'].append("$(" + name + "_SCRIPTS)")

  if (binsmap.has_key('HEADERS')):
    for h in HDRS:
	msc['HDRS'].append(h)

  msc_deps(fd,binsmap['DEPS'],".obj",msc);


def msc_library(fd, var, libmap, msc ):

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

  msc['LIBS'].append(sep+libname)
  if (libmap.has_key('MTSAFE')):
    fd.write("CFLAGS=$(CFLAGS) $(thread_safe_flag_spec)\n")

  if (libmap.has_key("LIBS")):
    fd.write(msc_additional_libs(libname, sep, "LIB", libmap["LIBS"],msc))

  for src in libmap['SOURCES']:
    base,ext = string.split(src,".", 1) 	
    if (ext not in automake_ext):
      msc['EXTRA_DIST'].append(src)
	
  srcs = libname+"_OBJS ="
  for target in libmap['TARGETS']:
    t,ext = string.split(target,".",1)
    if (ext == "o"):
      srcs = srcs + " " + t + ".obj" 
    elif (ext in hdrs_ext):
      HDRS.append(target)
    elif (ext in scripts_ext):
      if (target not in SCRIPTS):
        SCRIPTS.append(target)
  fd.write(srcs + "\n")
  fd.write( "lib"+sep+libname + ".dll: $(" + libname + "_OBJS)\n" )
  fd.write("\t$(CC) $(CFLAGS) -LD -Felib%s%s.dll $(%s_OBJS) $(lib%s_LIBS) $(LDFLAGS) /def:lib%s%s.def\n\n" % (libname,sep,libname,libname,sep,libname))

  if (len(SCRIPTS) > 0):
    fd.write(libname+"_SCRIPTS =" + msc_space_sep_list(SCRIPTS))
    msc['BUILT_SOURCES'].append("$(" + name + "_SCRIPTS)")

  if (libmap.has_key('HEADERS')):
    for h in HDRS:
	msc['HDRS'].append(h)

  msc_deps(fd,libmap['DEPS'],".obj",msc);

def msc_libs(fd, var, libsmap, msc ):

  if (libsmap.has_key('SEP')):
  	sep = libsmap['SEP'][0]

  SCRIPTS = []
  scripts_ext = []
  if (libsmap.has_key('SCRIPTS')):
    scripts_ext = libsmap['SCRIPTS']

  if (libsmap.has_key('MTSAFE')):
    fd.write("CFLAGS=$(CFLAGS) $(thread_safe_flag_spec)\n")

  for libsrc in libsmap['SOURCES']:
    lib,ext = string.split(libsrc,".", 1) 	
    if (ext not in automake_ext):
      msc['EXTRA_DIST'].append(libsrc)
    msc['LIBS'].append(sep+lib)
	
    if (libsmap.has_key(lib + "_LIBS")):
      fd.write(msc_additional_libs(lib, sep, "LIB", libsmap[lib + "_LIBS"],msc))
    elif (libsmap.has_key("LIBS")):
      fd.write(msc_additional_libs(lib, sep, "LIB", libsmap["LIBS"],msc))

    srcs = "lib"+sep+lib+"_OBJS ="
    for target in libsmap['TARGETS']:
      l = len(lib)
      if (target[0:l] == lib):
        t,ext = string.split(target,".",1)
        if (ext == "o"):
          srcs = srcs + " " + t + ".obj" 
        elif (ext in scripts_ext):
          if (target not in SCRIPTS):
            SCRIPTS.append(target)
    fd.write(srcs + "\n")
    fd.write( "lib"+sep+lib + ".dll: $(lib"+sep+lib+"_OBJS)\n" )
    fd.write("\t$(CC) $(CFLAGS) -LD -Felib%s%s.dll $(lib%s%s_OBJS) $(lib%s%s_LIBS) $(LDFLAGS) /def:lib%s%s.def \n\n" % (sep,lib,sep,lib,sep,lib,sep,lib))

  if (len(SCRIPTS) > 0):
    fd.write("SCRIPTS =" + msc_space_sep_list(SCRIPTS))
    msc['BUILT_SOURCES'].append("$(SCRIPTS)")

  if (libsmap.has_key('HEADERS')):
    HDRS = []
    hdrs_ext = libsmap['HEADERS']
    for target in libsmap['DEPS'].keys():
      t,ext = string.split(target,".",1)
      if (ext in hdrs_ext):
        msc['HDRS'].append(target)

  msc_deps(fd,libsmap['DEPS'],".obj",msc);

def msc_includes(fd, var, values, msc):
  incs = ""
  for i in values:
    if (i[0] == "-" or i[0] == "$"):
      	incs = incs + " " + i
    else:
	incs = incs + " -I" + msc_translate_dir(i,msc)
  fd.write("INCLUDES = " + incs + "\n")

output_funcs = { 'SUBDIRS': msc_subdirs, 
	 	 'EXTRA_DIST': msc_extra_dist,
	 	 'EXTRA_HEADERS': msc_extra_headers,
	 	 'LIBDIR': msc_libdir,
		 'LIBS' : msc_libs,
		 'LIB' : msc_library,
		 'BINS' : msc_bins,
		 'BIN' : msc_binary,
 		 'INCLUDES' : msc_includes,
		 'MTSAFE' : msc_mtsafe,
		 'CFLAGS' : msc_cflags,
		}


def output(tree, cwd, topdir):
  fd = open(cwd+os.sep+'Makefile.msc',"w")

  fd.write('''
## Use: nmake -f makefile.msc install

# Change this to wherever you want to install the DLLs. This directory
# should be in your PATH.
BIN = C:\\bin

################################################################

# Nothing much configurable below

# cl -? describes the options
CC = cl -G5 -GF -Ox -W3 -MD -nologo

# No general LDFLAGS needed
LDFLAGS = /link
INSTALL = copy
MKDIR = mkdir
ECHO = echo
CD = cd

CFLAGS = -I. -I$(TOPDIR) $(LIBC_INCS) -DHAVE_CONFIG_H

CXXEXT = \\\"cxx\\\"

''')

  msc = {}
  msc['BUILT_SOURCES'] = []
  msc['EXTRA_DIST'] = []
  msc['LIBS'] = []
  msc['BINS'] = []
  msc['HDRS'] = []
  msc['INSTALL'] = []
  msc['LIBDIR'] = 'lib'
  
  prefix = os.path.commonprefix([cwd,topdir])
  d = cwd[len(prefix):]
  reldir = "." 
  if (len(d) > 1 and d[0] == os.sep):
    d = d[1:]
    while(len(d) > 0):
	reldir = reldir + os.sep + ".."
	d,t = os.path.split(d)  

  fd.write("TOPDIR = %s\n" % regsub.gsub("/", "\\", reldir))
  fd.write("!INCLUDE $(TOPDIR)\\rules.msc\n")
  if ("SUBDIRS" in tree.keys()):
     fd.write("all: all-msc all-recursive\n")
     fd.write("install: install-recursive install-msc\n")
  else:
     fd.write("all: all-msc\n")
     fd.write("install: install-msc\n")
	 
  for i in tree.keys():
    j = string.upper(i[0:3])
    if (output_funcs.has_key(i)):
	output_funcs[i](fd,i,tree.value(i),msc)
    elif (output_funcs.has_key(j)):
	output_funcs[j](fd,i,tree.value(i),msc)
    elif (i != 'TARGETS'):
	msc_assignment(fd,i,tree.value(i),msc)

  fd.write("CFLAGS = $(INCLUDES) $(CFLAGS)\n" )
  fd.write("CXXFLAGS = $(CFLAGS)\n" )

  if (len(msc['BUILT_SOURCES']) > 0):
    fd.write("BUILT_SOURCES = ")
    for v in msc['BUILT_SOURCES']:
      fd.write(" %s" % (v) )
    fd.write("\n")

  #fd.write("EXTRA_DIST = Makefile.ag Makefile.msc")
  #for v in msc['EXTRA_DIST']:
    #fd.write(" %s" % (v) )
  #fd.write("\n")

  fd.write("all-msc:")
  if (topdir == cwd):
    fd.write(" config.h unistd.h sys\\socket.h")
  if (len(msc['LIBS']) > 0):
    for v in msc['LIBS']:
      fd.write(" lib%s.dll" % (v) )

  if (len(msc['BINS']) > 0):
    for v in msc['BINS']:
      fd.write(" %s.exe" % (v) )

  fd.write("\n")

  if (topdir == cwd):
      fd.write("config.h: winconfig.h\n\t$(INSTALL) winconfig.h config.h\n")
      fd.write("unistd.h: \n\t$(ECHO) #ifndef UNISTD_H > unistd.h\n\t$(ECHO) #define UNISTD_H >> unistd.h\n\t$(ECHO) #include \"io.h\" >> unistd.h\n\t$(ECHO) #endif >> unistd.h\n")
      fd.write("sys\\socket.h: \n\tif not exist sys $(MKDIR) sys\n\t$(ECHO) #ifndef SOCKET_H > sys\\socket.h\n\t$(ECHO) #define SOCKET_H >> sys\\socket.h\n\t$(ECHO) #include \"winsock.h\" >> sys\\socket.h\n\t$(ECHO) #endif >> sys\\socket.h\n")

  fd.write("install-msc: install-exec install-data\n")
  fd.write("install-exec: all\n")
  if (len(msc['LIBS']) > 0):
    for v in msc['LIBS']:
      fd.write("\t$(INSTALL) lib%s.dll $(%sdir)\n" % (v,msc['LIBDIR']) )
  if (len(msc['BINS']) > 0):
    for v in msc['BINS']:
      fd.write("\t$(INSTALL) %s.exe $(bindir)\n" % (v) )
  if (len(msc['INSTALL']) > 0):
    for (dst,src) in msc['INSTALL']:
      fd.write("\t$(INSTALL) $(bindir)/%s $(bindir)/%s\n" % (src,dst) )

  #fd.write("install-data:")
  #if (len(msc['HDRS']) > 0):
    #if (len(name) > 0): 
      #fd.write(" install-%s" % (v) )
      #for v in msc['HDRS']:
      #fd.write("%sincludedir = $(includedir)/%s\n" % (name,name))
    #fd.write("%sinclude_HEADERS = %s\n" % (name,am_list2string(am['HDRS']," ","")))
  #fd.write("\n")
