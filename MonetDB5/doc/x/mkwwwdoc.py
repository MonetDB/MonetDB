# WARNING THIS FILE IS OBSOLETE

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
# Portions created by CWI are Copyright (C) 1997-2004 CWI.
# All Rights Reserved.
# 
# Contributor(s):
# 		Martin Kersten <Martin.Kersten@cwi.nl>
# 		Peter Boncz <Peter.Boncz@cwi.nl>
# 		Niels Nes <Niels.Nes@cwi.nl>
# 		Stefan Manegold  <Stefan.Manegold@cwi.nl>

import sys, os, string

if len(sys.argv) != 3:
    print '''\
Usage: %s srcdir dstdir

where srcdir is the top of the MonetDB source directory tree, and
dstdir is the top of the installation tree.''' % sys.argv[0]
    sys.exit(1)

doctype= 'html'
srcdir = sys.argv[1]
dstdir = sys.argv[2]

# Here a hack to make this work on Windows:
# The command name cannot have any spaces, not even quoted, so we
# can't use (as we used to) os.path.join(dstdir, 'bin', 'Mx') for the
# value of mx.  Instead we add the bin directory to the beginning of
# the command search path.  We assume there is a PATH in the
# environment, but that doesn't seem like to big an assumption.
os.environ['PATH'] = os.path.join(dstdir, 'bin') + os.pathsep + os.environ['PATH']
mx = 'Mx'

# figure out a place for temporary files
import tempfile
tmpdir = tempfile.gettempdir()

def copyfile(srcfile, dstfile):
    print 'copyfile', srcfile, dstfile
    f = open(srcfile, 'rb')
    data = f.read()
    f.close()
    f = open(dstfile, 'wb')
    f.write(data)
    f.close()

def unlink(file):
    try:
        os.unlink(file)
    except os.error:
        pass

def runMx(srcdir, base, dstdir, suffix='' ):
    print 'runMx', srcdir, base, dstdir
    cwd = os.getcwd()
    print cwd, '->', srcdir
    os.chdir(srcdir)
    cmd = '%s "-R%s" -H1 -w -B "%s" 2>&1' % (mx, tmpdir, base + '.mx')
    print cmd
    f = os.popen(cmd, 'r')
    dummy = f.read()                    # discard output
    f.close()
    print srcdir, '->', cwd
    os.chdir(cwd)
    srcfile = os.path.join(tmpdir, base + '.body.'+doctype)
    if not os.path.exists(srcfile):
        srcfile = os.path.join(tmpdir, base + '.' + doctype)
    os.makedirs(os.path.join(dstdir, base+suffix))
    copyfile(srcfile, os.path.join(dstdir, base+suffix, 'index.' +doctype))
    unlink(os.path.join(tmpdir, base + '.' + doctype))
    unlink(os.path.join(tmpdir, base + '.index.'+doctype))
    unlink(os.path.join(tmpdir, base + '.body.'+doctype))

def removedir(dir):
    """Remove DIR recursively."""
    print 'removedir',dir
    if not os.path.exists(dir):
        return
    names = os.listdir(dir)
    for name in names:
        fn = os.path.join(dir, name)
        if os.path.isdir(fn):
            removedir(fn)
        else:
            os.remove(fn)
    os.rmdir(dir)

removedir(os.path.join(dstdir, 'doc', 'www'))

#for f in ['monet', 'mil', 'mel']:
#    runMx(os.path.join(srcdir, 'doc'), f, os.path.join(dstdir, 'doc', 'www', 'Services'))

#for f in ['monet.gif']:
#   d = string.split(f,'.')[0]
#   copyfile(os.path.join(srcdir, 'doc', f),
#            os.path.join(dstdir, 'doc', 'www', 'Services', d, f))


runMx(os.path.join(srcdir, 'src', 'gdk'), 'gdk',
  os.path.join(dstdir, 'doc', 'www', 'Services'))
for f in ['bat.gif', 'bat1.gif', 'bat2.gif']:
   copyfile(os.path.join(srcdir, 'src', 'gdk', f),
            os.path.join(dstdir, 'doc', 'www', 'Services', 'gdk', f))

#these files should be linked
for f in ['mal_atom',
    'mal_box',
    'mal_client',
    'mal_debugger',
    'mal_function',
    'mal_import',
    'mal_instruction',
    'mal_interpreter',
    'mal_linker',
    'mal',
    'mal_namespace',
    'mal_optimizer',
    'mal_parser',
    'mal_profiler',
    'mal_properties',
    'mal_resolve',
    'mal_scenario',
    'mal_scope',
    'mal_session',
    'mal_stack',
    'mal_startup',
    'mal_type',
    'mal_xml',
	]:
    runMx(os.path.join(srcdir, 'src', 'mal'), f,
          os.path.join(dstdir, 'doc', 'www', 'MAL'))

#runMx(os.path.join(srcdir, 'src', 'monet'), 'monet',
#     os.path.join(dstdir, 'doc', 'www'))
#copyfile(os.path.join(srcdir, 'src', 'monet', 'monet.gif'),
#        os.path.join(dstdir, 'doc', 'www', 'monet', 'monet.gif'))

runMx(os.path.join(srcdir, 'src', 'tools'), 'Mserver',
      os.path.join(dstdir, 'doc', 'www'))

# client documentation
runMx(os.path.join(srcdir, 'src', 'mapi', 'clients', 'C'), 'Mapi',
      os.path.join(dstdir, 'doc', 'www', 'APIs'), 'C')

runMx(os.path.join(srcdir, 'src', 'mapi', 'clients', 'C'), 'MapiClient',
      os.path.join(dstdir, 'doc', 'www'))

#runMx(os.path.join(srcdir, 'src', 'mapi'), 'mapi',
#     os.path.join(dstdir, 'doc', 'www', 'Modules'))

#runMx(os.path.join(srcdir, 'src', 'modules', 'calibrator'), 'calib',
#     os.path.join(dstdir, 'doc', 'www', 'Modules'))

#for f in ['aggrX3', 'aggr', 'alarm', 'algebra', 'arith', 'ascii_io', 'bat',
#          'blob', 'counters', 'decimal', 'enum', 'kernel',
#          'lock', 'mmath', 'monettime', 'pcl', 'radix', 'streams', 'str', 'sys',
#          'tcpip', 'trans', 'unix', 'url', 'xtables']:
#    runMx(os.path.join(srcdir, 'src', 'modules', 'plain'), f,
#          os.path.join(dstdir, 'doc', 'www', 'Modules'))

#for f in ['bitset', 'bitvector', 'ddbench', 'mel', 'mprof', 'oo7', 'qt', 'tpcd',
#          'wisc']:
#    runMx(os.path.join(srcdir, 'src', 'modules', 'contrib'), f,
#          os.path.join(dstdir, 'doc', 'www', 'Modules'))

#for f in ['README', 'load.mil', 'init.mil']:
#    copyfile(os.path.join(srcdir, 'scripts', 'gold', f),
#             os.path.join(dstdir, 'doc', 'www', f))
#for f in ['HowToStart', 'HowToStart-Win32.txt']:
#    copyfile(os.path.join(srcdir, f),
#             os.path.join(dstdir, 'doc', 'www', f))

copyfile('overview.shtml',
	os.path.join(dstdir, 'doc/www', 'overview.shtml'))

f = open(os.path.join(dstdir, 'doc', 'www', 'sql.html'), 'w')
f.write('''\
<html>
<body>
<h3>
 <a href="mailto:niels@cwi.nl">The SQL frontend is not documented yet</a>
</h3>
</body>
   </html>
''')
f.close()
