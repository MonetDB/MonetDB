
import string
import regex,regsub
import fileinput
import os
import shelve
from var import *

mx2mil = "^@mil[ \t]*"
mx2mel = "^@m[ \t]*"
mx2h = "^@h[ \t]*"
mx2c = "^@c[ \t]*"
mx2y = "^@y[ \t]*"
mx2l = "^@l[ \t]*"
mx2cc = "^@C[ \t]*"
mx2yy = "^@Y[ \t]*"
mx2ll = "^@L[ \t]*"
mx2odl = "^@odl[ \t]*"

e_mx = regex.compile('^@[^\{\}]')

code_extract = { 'mx': [ (mx2mil, '.mil'),
		  (mx2mel, '.m'), 
		  (mx2cc, '.cc'), 
		  (mx2c, '.c'), 
		  (mx2h, '.h'), 
		  (mx2y, '.y'), 
		  (mx2l, '.l'), 
		  (mx2yy, '.yy'), 
		  (mx2ll, '.ll'), 
		  (mx2odl, '.odl') ]  
}
end_code_extract = { 'mx': e_mx }

code_gen = { 'm': 	[ '.proto.h', '.glue.c' ],
	    'odl': 	[ '_odl.h', '_odl.cc', '_mil.cc', '_odl.m' ],
	    'y': 	[ '.tab.c', '.tab.h' ],
	    'tab.c': 	[ '.tab.o' ],
	    'l': 	[ '.yy.c' ], 
	    'yy.c': 	[ '.yy.o' ],
	    'yy': 	[ '.tab.cc', '.tab.h' ],
	    'tab.cc': 	[ '.tab.o' ],
	    'll': 	[ '.yy.cc' ], 
	    'yy.cc': 	[ '.yy.o' ],
	    'cc': 	[ '.o' ],
	    'c': 	[ '.o' ],
	    'glue.c': 	[ '.glue.o' ]
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
	 'y': [ c_inc, None, '' ], 
	 'yy': [ c_inc, None, '' ], 
	 'l': [ c_inc, None, '' ], 
	 'll': [ c_inc, None, '' ], 
	 'm': [ m_use, m_sep, '.m' ]
}

dep_map = { 'glue.c': [ '.proto.h' ],
	    'glue.o': [ '.proto.h' ]
}

dep_rules = { 'glue.c': [ 'm', '.proto.h' ] , 
	      'proto.h': [ 'm', '.proto.h']
}

lib_map = [ 'glue.c', 'm' ]

def readfile(f):
    src = open(f, 'rb')
    buf = src.read()
    src.close()
    return buf

def readfilepart(f,ext):
    dir,file = os.path.split(f)
    fn,fext = string.split(file,".", 1)
    src = open(f, 'rb')
    buf = src.read()
    src.close()
    buf2 = ""
    if (code_extract.has_key(fext)):
      e = end_code_extract[fext]
      for pat,newext in code_extract[fext]:
	s = regex.compile(pat)
	if (newext == "." + ext):
	  m = s.search(buf)
          while(m>=0):
	    n = e.search(buf,m+1)
	    if (n >= 0):
	    	buf2 = buf2 + buf[m:n]
	    	m = s.search(buf,n)
	    else:
	    	buf2 = buf2 + buf[m:]
		m = n
    return buf2

def do_code_extract(f,base,ext, targets, deps, cwd):
	b = readfile(cwd+os.sep+f)
	if (code_extract.has_key(ext)):
	  for pat,newext in code_extract[ext]:
	    p = regex.compile(pat)
	    if (p.search(b) >= 0 ):
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
		deps[newtarget].append(f)
	  else:
	  	deps[newtarget] = [ f ]
      else:
        ntargets.append(f)
    targets = ntargets
  return targets
		  
	

def find_org(deps,f):
  org = f
  while (deps.has_key(org)):	
	org = deps[org][0] #gen code is done first, other deps are appended 
  return org

def do_deps(targets,deps,includes,incmap,cwd):
  cache = {}
  do_scan(targets,deps,incmap,cwd,cache)
  do_dep_rules(deps,cwd,cache)
  do_dep_combine(deps,includes,cwd,cache)
  cachefile = cwd + os.sep + '.cache'
  if os.path.exists( cachefile ):
    os.unlink(cachefile)
  cache_store = shelve.open( cachefile, "c")
  for k,vals in cache.items():
	cache_store[k] = vals
  cache_store.close()

def do_recursive_combine(deplist,cache,depfiles):
  for d in deplist:
    if (cache.has_key(d)):
      if (d not in depfiles):
        depfiles.append(d)
        do_recursive_combine(cache[d],cache,depfiles)

def do_dep_combine(deps,includes,cwd,cache):
  for target,depfiles in deps.items():
    for d in depfiles:
      if (includes.has_key(d)):
	for f in includes[d]:
	  if (f not in depfiles):
	    depfiles.append(f)
      elif (cache.has_key(d)):
	do_recursive_combine(cache[d],cache,depfiles)

def do_dep_rules(deps,cwd,cache):
  for target in deps.keys():
    tf,te = string.split(target,".",1)
    if (dep_rules.has_key(te)):
      (dep,new) = dep_rules[te]
      if (cache.has_key(tf+"."+dep)):
        if (tf+new not in cache[target]):
      	  cache[target].append(tf+new)
	for d in cache[tf+"."+dep]:
          df,de = string.split(d,".",1)
	  if (de == dep and df+new not in cache[target]):
      	    cache[target].append(df+new)

def do_scan(targets,deps,incmap,cwd,cache):
  for target,depfiles in deps.items():
      base,ext = string.split(target,".", 1)
      if (not cache.has_key(target)):
        inc_files = []
        if (scan_map.has_key(ext)):
          org = cwd+os.sep+find_org(deps,target)
    	  if os.path.exists(org):
            b = readfilepart(org,ext)
            pat,sep,depext = scan_map[ext]
            m = pat.search(b)
            while (m >= 0):
	      fnd = pat.group(1)
	      if (sep):
                p = 0
                n = sep.search(fnd)
	        while (n > 0):
	          sepm = sep.group(0)
                  fnd1 = fnd[p:n]
	          p = n+len(sepm)
	          if (deps.has_key(fnd1+depext) or fnd1+depext in targets):
	            if (fnd1+depext not in inc_files):
		      inc_files.append(fnd1+depext)
 		  elif (incmap.has_key(fnd1+depext)):
	            if (fnd1+depext not in inc_files):
		      inc_files.append(incmap[fnd1+depext]+os.sep+fnd1+depext)
	          n = sep.search(fnd,p)
	      if (deps.has_key(fnd+depext) or fnd+depext in targets):
	        if (fnd+depext not in inc_files):
		  inc_files.append(fnd+depext)
 	      elif (incmap.has_key(fnd+depext)):
	        if (fnd+depext not in inc_files):
		  inc_files.append(incmap[fnd+depext]+os.sep+fnd+depext)
 	      #else:
		#print(fnd + " not in deps and incmap " + depext )
              m = pat.search(b,m+1)
        cache[target] = inc_files
  #for i in cache.keys():
    #print(i,cache[i])

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
    if (dir[0:2] == ".."):
    	f = cwd + os.sep + dir + os.sep + ".cache"
    else:
    	f = dir + os.sep + ".cache"
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
 
  deps = {}
  for i in tree.keys():  
    if ( i[0:4] == "lib_" or i[0:4] == "bin_" or i[0:4] == "mod_" or \
	 i == "LIBS" or i == "BINS" or i == "MODS" ):
	  targets = []
 	  if (type(tree.value(i)) == type({}) ):
	    for f in tree.value(i)["SOURCES"]:
	      (base,ext) = string.split(f,".", 1)
	      do_code_extract(f,base,ext, targets, deps, cwd)
	    targets = do_code_gen(targets,deps)
	    do_deps(targets,deps,includes,incmap,cwd)
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
	    elif (i == "LIBS"):
	      for l,d in libs.items():
		n,dummy = string.split(l,"_",1)
	        tree.value(i)[n+'_DLIBS'] = d
		#if (tree.value(i).has_key(l)):
		  # for dep in d:
		    # tree.value(i)[l].append(dep)
		#else:
	        #  tree.value(i)[l] = d
	        #  if (tree.value(i).has_key('LIBS')):
		#     for dep in tree.value(i)['LIBS']:
		#       tree.value(i)[l].append(dep)
	    else:
	      for l,d in libs.items():
	        tree.value(i)[l] = d
