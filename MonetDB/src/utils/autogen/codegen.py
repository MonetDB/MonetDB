
import string
import regex,regsub
import fileinput
import os
import shelve
from var import *

mx2mil = "^@mil[ \t]*$"
mx2mel = "^@m[ \t]*$"
mx2h = "^@h[ \t]*$"
mx2c = "^@c[ \t]*$"
mx2y = "^@y[ \t]*$"
mx2l = "^@l[ \t]*$"
mx2cc = "^@C[ \t]*$"
mx2yy = "^@Y[ \t]*$"
mx2ll = "^@L[ \t]*$"
mx2odl = "^@odl[ \t]*$"

code_extract = { 'mx': [ (mx2mil, '.mil'), 
		  (mx2mel, '.m'), 
		  (mx2cc, '.cc'), 
		  (mx2c, '.c'), 
		  (mx2h, '.h'), 
		  (mx2y, '_tab.y'), 
		  (mx2l, '_yy.l'), 
		  (mx2yy, '_tab.yy'), 
		  (mx2ll, '_yy.ll'), 
		  (mx2odl, '.odl') ] 
}

code_gen = { 'm': [ '.proto.h', '.glue.c' ],
	    'odl': [ '_odl.h', '_odl.cc', '_mil.cc', '_odl.m' ],
	    'y': [ '.c', '.h' ],
	    'l': [ '.c' ], 
	    'yy': [ '.cc', '.h' ],
	    'll': [ '.cc' ], 
	    'cc': [ '.o' ],
	    'c': [ '.o' ],
	    'glue.c': [ '.glue.o' ]
}

c_inc = "^[ \t]*#[ \t]*include[ \t]*[<\"]\([a-zA-Z0-9\.\_]*\)[>\"]"
m_use = "^[ \t]*\.[Uu][Ss][Ee][ \t]+\([a-zA-Z0-9\.\_, ]*\);"
m_sep = "[ \t]*,[ \t*]"

c_inc = regex.compile(c_inc)
m_use = regex.compile(m_use)
m_sep = regex.compile(m_sep)

scan_map = { 'c': [ c_inc, None, '' ], 
	 'cc': [ c_inc, None, '' ], 
	 'h': [ c_inc, None, '' ], 
	 'm': [ m_use, m_sep, '.m' ]
}

dep_map = { 'glue.c': [ '.proto.h' ]
}

lib_map = [ 'glue.c', 'm' ]

def readfile(f):
    src = open(f, 'rb')
    buf = src.read()
    src.close()
    return buf

def do_code_extract(f,base,ext, targets, deps, cwd):
	b = readfile(cwd+os.sep+f)
	v = var(base + "." + ext)
	if (code_extract.has_key(ext)):
	  for pat,newext in code_extract[ext]:
	    p = regex.compile(pat);
	    if (p.search(b) > 0 ):
	      extracted = base + newext
	      targets.append( extracted )
	      deps[extracted] = [ f ]
 	else:
	  targets.append(f)

def do_code_gen(targets, deps):
  changes = 1
  while(changes):
    ntargets = []
    changes = 0
    for f in targets:
      base,ext = string.split(f,".", 1)
      if (code_gen.has_key(ext)):
	changes = 1
	for newext in code_gen[ext]:
	  newtarget = base + newext  
          ntargets.append(newtarget)
	  if (deps.has_key(newtarget)):
		deps[newtarget] = deps[newtarget].append(f)
	  else:
	  	deps[newtarget] = [ f ]
      else:
        ntargets.append(f)
    targets = ntargets
  return targets
		  
	

def find_org(deps,f):
  org = f;
  while (deps.has_key(org)):	
	org = deps[org][0] #gen code is done first, other deps are appended 
  return org

c_extern_func = "^[ \t]*extern[ \t].*[ \t\*]\([a-zA-Z0-9\_]+\)[ \t]*("
c_extern_func = regex.compile(c_extern_func)

c_extern_var = "^[ \t]*extern[ \t]*[^( \t]*[ \t\*]*\([a-zA-Z0-9\_]+\)[;\[]"
c_extern_var = regex.compile(c_extern_var)

def do_defs(targets,deps):
  defs = []
  for f in targets:
    org = find_org(deps,f);
    b = readfile(org)
    m = c_extern_func.search(b)
    while(m > 0):
	fnd = c_extern_func.group(1)
	if (fnd not in defs):
		defs.append(fnd)
        m = c_extern_func.search(b,m+1)
    m = c_extern_var.search(b)
    while(m > 0):
	fnd = c_extern_var.group(1)
	if (fnd not in defs):
		defs.append(fnd)
        m = c_extern_var.search(b,m+1)
  print("EXPORTS")
  for i in defs:
	print ("\t%s" % i)

def do_deps(deps,includes,incmap,cwd):
  cachefile = cwd + os.sep + '.cache'
  if os.path.exists( cachefile ):
    os.unlink(cachefile)
  cache = shelve.open( cachefile, "c")
  for target,depfiles in deps.items():
    for i in depfiles:
      if (includes.has_key(i)):
	for f in includes[i]:
	  if (f not in depfiles):
	    depfiles.append(f)
      elif (cache.has_key(i)):
	for f in cache[i]:
	  if (f not in depfiles):
	    depfiles.append(f)
      else:
        base,ext = string.split(i,".", 1)
	inc_files = []
        if (dep_map.has_key(ext)):
	  for depext in dep_map[ext]:
	    if (base+depext not in depfiles):
	      depfiles.append(base+depext)
	    if (base+depext not in inc_files):
	      inc_files.append(base+depext)
        if (scan_map.has_key(ext)):
          org = cwd+os.sep+find_org(deps,i)
    	  if os.path.exists(org):
            b = readfile(org)
            pat,sep,depext = scan_map[ext]
            m = pat.search(b)
            while (m > 0):
	      fnd = pat.group(1)
	      if (sep):
                p = 0;
                n = sep.search(fnd)
	        while (n > 0):
	          sepm = sep.group(0)
                  fnd1 = fnd[p:n]
	          p = n+len(sepm)
	          if (deps.has_key(fnd1+depext)):
	            if (fnd1+depext not in depfiles):
		      depfiles.append(fnd1+depext)
	            if (fnd1+depext not in inc_files):
		      inc_files.append(fnd1+depext)
 		  elif (incmap.has_key(fnd1+depext)):
	            if (fnd1+depext not in depfiles):
		      depfiles.append(incmap[fnd1+depext]+os.sep+fnd1+depext)
	            if (fnd1+depext not in inc_files):
		      inc_files.append(incmap[fnd1+depext]+os.sep+fnd1+depext)
	          n = sep.search(fnd,p)
	      if (deps.has_key(fnd+depext)):
	        if (fnd+depext not in depfiles):
	          depfiles.append(fnd+depext)
	        if (fnd+depext not in inc_files):
		  inc_files.append(fnd+depext)
 	      elif (incmap.has_key(fnd+depext)):
	        if (fnd+depext not in depfiles):
	          depfiles.append(incmap[fnd+depext]+os.sep+fnd+depext)
	        if (fnd+depext not in inc_files):
		  inc_files.append(incmap[fnd+depext]+os.sep+fnd+depext)
              m = pat.search(b,m+1)
          cache[i] = inc_files
  cache.close()
	
# move to am
def do_lib(lib,deps):
  true_deps = []
  base,ext = string.split(lib,".", 1)
  if (ext in lib_map):
    if (deps.has_key(lib)):
      lib_deps = deps[lib]
      for d in lib_deps:
      	b,ext = string.split(d,".", 1)
	if (base != b):
      	  if (ext in lib_map):
	    if (b not in true_deps):
	      true_deps.append("lib_"+b)
	    n_libs = do_lib(d,deps)
	    for l in n_libs:
	      if (l not in true_deps):
	        true_deps.append(l)
  return true_deps

def do_libs(deps):
  libs = {}
  for target,depfiles in deps.items():
    lib = target
    while(1):
      true_deps = do_lib(lib,deps)
      if (len(true_deps) > 0):
        base,ext = string.split(lib,".", 1)
	libs[base+"_LIBS"] = true_deps
      if (deps.has_key(lib)):
        lib = deps[lib][0]
      else:
	break 
  return libs

def read_depsfile(incdirs, cwd, topdir):
  includes = {}
  incmap = {}
  for i in incdirs:
    dir = i
    if (string.find(i,os.sep) >= 0):
      d,rest = string.split(i,os.sep, 1) 
      if (d == "top_srcdir" or d == "top_builddir"):
	dir = topdir + os.sep + rest
      elif (d == "srcdir" or d == "builddir"):
	dir = rest
    f = cwd + os.sep + dir + os.sep + ".cache"
    lineno = 0
    if os.path.exists(f):
        cache = shelve.open(f)
	for d in cache.keys():
	  inc = []
	  for dep in cache[d]:
	    if (string.find(dep,os.sep) < 0):
	      dep = i+os.sep+dep
            inc.append(dep)
	  includes[i+os.sep+d] = inc
	  incmap[d] = i
        cache.close()
  return includes,incmap

# todo (change to a class)
def codegen(tree, cwd, topdir):
  includes = {}
  incmap = {}
  if ("INCLUDES" in tree.keys()):
    includes,incmap = read_depsfile(tree.value("INCLUDES"),cwd, topdir)
  for i in tree.keys():  
    if ( i[0:4] == "lib_" or i[0:4] == "bin_" or i[0:4] == "mod_" or \
	 i == "LIBS" or i == "BINS" or i == "MODS" ):
	  targets = []
	  deps = {}
 	  if (type(tree.value(i)) == type({}) ):
	    for f in tree.value(i)["SOURCES"]:
	      (base,ext) = string.split(f,".", 1)
	      do_code_extract(f,base,ext, targets, deps, cwd)
	    targets = do_code_gen(targets,deps)
            #do_defs(targets,deps)
	    do_deps(deps,includes,incmap,cwd)
	    libs = do_libs(deps)
      	    tree.value(i)["TARGETS"] = targets
      	    tree.value(i)["DEPS"] = deps
		 
	    if (i[0:4] == "lib_"):
	      if (libs.has_key(i[4:]+"_LIBS")):
	        d = libs[i[4:]+"_LIBS"]
	        if (tree.value(i).has_key('LIBS')):
		  for l in d:
	            tree.value(i)['LIBS'].append(l)
	        else:
	          tree.value(i)['LIBS'] = d
	    else:
	      for l,d in libs.items():
	        tree.value(i)[l] = d
