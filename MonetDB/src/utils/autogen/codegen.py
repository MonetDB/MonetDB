
import string
import regex,regsub
import fileinput
import os
import shelve
from var import *

mx2mil = "^@mil[ \t\r\n]+"
mx2mel = "^@m[ \t\r\n]+"
mx2h = "^@h[ \t\r\n]+"
mx2c = "^@c[ \t\r\n]+"
mx2y = "^@y[ \t\r\n]+"
mx2l = "^@l[ \t\r\n]+"
mx2cc = "^@C[ \t\r\n]+"
mx2yy = "^@Y[ \t\r\n]+"
mx2ll = "^@L[ \t\r\n]+"
mx2odl = "^@odl[ \t\r\n]+"
mx2fgr = "^@fgr[ \t\r\n]+"
mx2cfg = "^@cfg[ \t\r\n]+"
mx2tcl = "^@tcl[ \t\r\n]+"
mx2swig = "^@swig[ \t\r\n]+"
mx2java = "^@java[ \t\r\n]+"
mx2xsl = "^@xsl[ \t\r\n]+"
mx2sh = "^@sh[ \t\r\n]+"
mx2tex = "^@T[ \t\r\n]+"
mx2html = "^@T[ \t\r\n]+"

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
		  (mx2odl, '.odl'),
		  (mx2cfg, '.cfg'), 
		  (mx2fgr, '.fgr'), 
		  (mx2tcl, '.tcl'), 
		  (mx2swig, '.i'), 
		  (mx2java, '.java'), 
		  (mx2xsl, '.xsl'), 
		  (mx2sh, ''), 
		  (mx2tex, '.tex'), 
		  (mx2html, '.html'), ], 
 		'mx.in': [ (mx2mil, '.mil'),
		  (mx2mel, '.m'), 
		  (mx2cc, '.cc'), 
		  (mx2c, '.c'), 
		  (mx2h, '.h'), 
		  (mx2y, '.y'), 
		  (mx2l, '.l'), 
		  (mx2yy, '.yy'), 
		  (mx2ll, '.ll'), 
		  (mx2odl, '.odl'),
		  (mx2fgr, '.fgr'), 
		  (mx2cfg, '.cfg'), 
		  (mx2tcl, '.tcl'), 
		  (mx2swig, '.i'), 
		  (mx2java, '.java'), 
		  (mx2xsl, '.xsl'), 
		  (mx2sh, ''), 
		  (mx2tex, '.tex'), 
		  (mx2html, '.html'), ] 
}
end_code_extract = { 'mx': e_mx, 'mx.in': e_mx }

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
	    'i':	[ '_wrap.c' ],
	    'glue.c': 	[ '.glue.o' ],
	    'fgr':     [ '_engine.c', '_proto.h' ],
	    'java':   [ '.class' ],
	    'mx.in':	[ '.mx' ],
	    'tex':	[ '.dvi' ],
	    'dvi':	[ '.ps' ],
	    'fig':	[ '.eps' ],
	    'feps':	[ '.eps' ],
	    'in':	[ '' ],
	    'cfg.in':	[ '.cfg' ],
	    'java.in':	[ '.java' ],
}

c_inc = "^[ \t]*#[ \t]*include[ \t]*[<\"]\([a-zA-Z0-9\.\_]*\)[>\"]"
m_use = "^[ \t]*\.[Uu][Ss][Ee][ \t]+\([a-zA-Z0-9\.\_, ]*\);"
m_sep = "[ \t]*,[ \t*]"
xsl_inc = "^[ \t]*<xsl:{include|import}[ \t]*href=['\"]\([a-zA-Z0-9\.\_]*\)['\"]"
tex_inc = ".*\\epsffile\{\([a-zA-Z0-9\.\_]*\)"

c_inc = regex.compile(c_inc)
m_use = regex.compile(m_use)
m_sep = regex.compile(m_sep)
xsl_inc = regex.compile(xsl_inc)
tex_inc = regex.compile(tex_inc)

scan_map = { 'c': [ c_inc, None, '' ], 
	 'cc': [ c_inc, None, '' ], 
	 'h': [ c_inc, None, '' ], 
	 'y': [ c_inc, None, '' ], 
	 'yy': [ c_inc, None, '' ], 
	 'l': [ c_inc, None, '' ], 
	 'll': [ c_inc, None, '' ], 
	 'm': [ m_use, m_sep, '.m' ],
	 'xsl': [ xsl_inc, None, '' ], 
	 'tex': [ tex_inc, None, '' ], 
}

dep_rules = { 'glue.c': [ 'm', '.proto.h' ] , 
	      'proto.h': [ 'm', '.proto.h']
}

lib_map = [ 'glue.c', 'm' ]

def split_filename(f): 
	base = f
	ext = ""
	if (string.find(f,".") >= 0):
		return string.split(f,".", 1)
	return (base,ext)
	

def readfile(f):
    src = open(f, 'rb')
    buf = src.read()
    src.close()
    return buf

def readfilepart(f,ext):
    dir,file = os.path.split(f)
    fn,fext = split_filename(file)
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

# targets are all objects which can somehow be created.
# In the code extraction phase targets are created from files
# possibly containing multiple targets
#
# The do_code_extract also tracks the files from which to extract
# these targets , ie. the dependencies.
def do_code_extract(f,base,ext, targets, deps, cwd):
        file = cwd+os.sep+f
    	if os.path.exists(file):
	  b = readfile(file)
	  if (code_extract.has_key(ext)):
	    for pat,newext in code_extract[ext]:
	      p = regex.compile(pat)
	      if (p.search(b) >= 0 ):
	        extracted = base + newext
	        targets.append( extracted )
	        deps[extracted] = [ f ]
 	  else:
	    targets.append(f)
 	else:
	  targets.append(f)
  
# In the code generation phase targets are generated using input files
# depending on the extention a target is generated
#
# The do_code_gen also tracks the input files for the code extraction
# of these targets , ie. the dependencies.
def do_code_gen(targets, deps):
  changes = 1
  while(changes):
    ntargets = []
    changes = 0
    for f in targets:
      base,ext = split_filename(f)
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

# do_deps finds the dependencies for the given list of targets
# based on the includes (or alike)
#
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

def do_recursive_combine(deplist,includes,cache,depfiles):
  for d in deplist:
    if (includes.has_key(d)):
      for f in includes[d]:
        if (f not in depfiles):
	  depfiles.append(f)
      # need to add include d too
      if (d not in depfiles):
        depfiles.append(d)
    elif (cache.has_key(d)):
      if (d not in depfiles):
        depfiles.append(d)
        do_recursive_combine(cache[d],includes,cache,depfiles)

# combine the found dependencies, ie. transitive closure.
def do_dep_combine(deps,includes,cwd,cache):
  for target,depfiles in deps.items():
    for d in depfiles:
      if (cache.has_key(d)):
	do_recursive_combine(cache[d],includes,cache,depfiles)
    # remove recursive dependencies (target depends somehow on itself)
    if target in depfiles:
	depfiles.remove(target)

# dependency rules describe two forms of dependencies, first
# are implicite rules, glue.c depends on proto.h and
# second dependency between generated targets based on 
# dependencies between the input files for the target generation process 
# ie. io.m, generates io_glue.c which and depends on io_proto.h)
def do_dep_rules(deps,cwd,cache):
  for target in deps.keys():
    tf,te = split_filename(target)
    if (dep_rules.has_key(te)):
      (dep,new) = dep_rules[te]
      if (cache.has_key(tf+"."+dep)):
        if (tf+new not in cache[target]):
      	  cache[target].append(tf+new)
	for d in cache[tf+"."+dep]:
          df,de = split_filename(d)
	  if (de == dep and df+new not in cache[target]):
      	    cache[target].append(df+new)
      else:
      	  cache[target].append(tf+new)

# scan for includes and match against the known deps and include map.
def do_scan(targets,deps,incmap,cwd,cache):
  for target,depfiles in deps.items():
      base,ext = split_filename(target)
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
		#print(fnd + depext + " not in deps and incmap " )
              m = pat.search(b,m+1)
        cache[target] = inc_files
  #for i in cache.keys():
    #print(i,cache[i])

def do_lib(lib,deps):
  true_deps = []
  base,ext = split_filename(lib)
  if (ext in lib_map):
    if (deps.has_key(lib)):
      lib_deps = deps[lib]
      for d in lib_deps:
	dirname = os.path.dirname(d)
      	b,ext = split_filename(os.path.basename(d))
	if (base != b):
      	  if (ext in lib_map):
	    if (b not in true_deps):
	      if (len(dirname) > 0):
	      	true_deps.append("-l_"+b)
	      else:
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
        base,ext = split_filename(lib)
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
    if (i[0:2] == "-I"):
      i = i[2:]
    dir = i
    if (dir[0:2] == "$("):
      var, rest = string.split(dir[2:], ')');
      if (os.environ.has_key( var )):
	value = os.environ[var]
	dir = value + rest
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
    else:
	if (i[0:2] == "-I"):
		i = i[2:]
	if (i[0:2] == "$("):
		var, rest = string.split(i[2:], ')');
		if (os.environ.has_key( var )):
			value = os.environ[var]
			i = value + rest
    	if os.path.exists(i):
		for dep in os.listdir(i):
	  		includes[i+os.sep+dep] = [ i+os.sep+dep ]
			incmap[dep] = i

  return includes,incmap

def codegen(tree, cwd, topdir):
  includes = {}
  incmap = {}
  if ("INCLUDES" in tree.keys()):
    includes,incmap = read_depsfile(tree.value("INCLUDES"),cwd, topdir)
 
  deps = {}
  for i in tree.keys():  
    if ( i[0:4] == "lib_" or i[0:4] == "bin_" or  i[0:4] == "doc_" or \
         i == "LIBS" or i == "BINS" or i[0:8] == "scripts_" ):
	  targets = []
 	  if (type(tree.value(i)) == type({}) ):
	    for f in tree.value(i)["SOURCES"]:
	      (base,ext) = split_filename(f)
	      do_code_extract(f,base,ext, targets, deps, cwd)
	    targets = do_code_gen(targets,deps)
	    do_deps(targets,deps,includes,incmap,cwd)
	    libs = do_libs(deps)
      	    tree.value(i)["TARGETS"] = targets
      	    tree.value(i)["DEPS"] = deps
		 
	    if (i[0:4] == "lib_"):
	      lib = i[4:] + "_LIBS"
	      if (lib[0] == "_"):
		lib = lib[1:]
	      if (libs.has_key(lib)):
	        d = libs[lib]
	        if (tree.value(i).has_key('LIBS')):
		  for l in d:
	            tree.value(i)['LIBS'].append(l)
	        else:
	          tree.value(i)['LIBS'] = d
	    elif (i == "LIBS"):
	      for l,d in libs.items():
		n,dummy = string.split(l,"_",1)
	        tree.value(i)[n+'_DLIBS'] = d
	    else:
	      for l,d in libs.items():
	        tree.value(i)[l] = d
