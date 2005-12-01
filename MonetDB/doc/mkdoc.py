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

import sys, os, string

if len(sys.argv) != 4:
    print '''\
Usage: %s srcdir blddir dstdir

where srcdir is the top of the MonetDB source directory tree, and
dstdir is the top of the installation tree.''' % sys.argv[0]
    sys.exit(1)

srcdir = sys.argv[1]
blddir = sys.argv[2]
dstdir = sys.argv[3]

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
    try:
        f = open(srcfile, 'rb')
    except IOError, (IOerrNo, IOerrStr):
        print "! mkdoc.py: copyfile: Opening file '%s' in mode 'rb' failed with #%d: '%s' !" % (srcfile, IOerrNo, IOerrStr)
        return
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
    cmd = '%s "-R%s" -H1 -w "%s" 2>&1' % (mx, tmpdir, base + '.mx')
    print cmd
    f = os.popen(cmd, 'r')
    dummy = f.read()                    # discard output
    f.close()
    print srcdir, '->', cwd
    os.chdir(cwd)
    srcfile = os.path.join(tmpdir, base + '.body.html')
    if not os.path.exists(srcfile):
        srcfile = os.path.join(tmpdir, base + '.html')
    os.makedirs(os.path.join(dstdir, base+suffix))
    copyfile(srcfile, os.path.join(dstdir, base+suffix, 'index.html'))
    unlink(os.path.join(tmpdir, base + '.html'))
    unlink(os.path.join(tmpdir, base + '.index.html'))
    unlink(os.path.join(tmpdir, base + '.body.html'))

def runMxTexi(srcdir, base, dstdir, suffix='' ):
    print 'runMxTexi', srcdir, base, dstdir
    cwd = os.getcwd()
    print cwd, '->', srcdir
    os.chdir(srcdir)
    cmd = '%s "-R%s" -H1 -i "%s" 2>&1 && cd "%s" && texi2html -nomenu -nosec_nav "%s"' % (mx, tmpdir, base + '.mx', tmpdir, base + '.texi')
    print cmd
    f = os.popen(cmd, 'r')
    dummy = f.read()                    # discard output
    f.close()
    print srcdir, '->', cwd
    os.chdir(cwd)
    srcfile = os.path.join(tmpdir, base + '.html')
    os.makedirs(os.path.join(dstdir, base+suffix))
    copyfile(srcfile, os.path.join(dstdir, base+suffix, 'index.html'))
    unlink(os.path.join(tmpdir, base + '.html'))
    unlink(os.path.join(tmpdir, base + '.texi'))

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

removedir(os.path.join(dstdir, 'doc', 'MonetDB'))
copyfile(os.path.join(srcdir, 'doc', 'MonetDB.html'),
         os.path.join(dstdir, 'doc', 'MonetDB.html'))

for f in ['mil']:
    runMx(os.path.join(srcdir, 'doc'), f, os.path.join(dstdir, 'doc', 'MonetDB', 'TechDocs', 'FrontEnds'))
for f in ['monet', 'mel']:
    runMx(os.path.join(srcdir, 'doc'), f, os.path.join(dstdir, 'doc', 'MonetDB', 'TechDocs', 'Core'))
for f in ['monet.gif', 'mel.gif']:
    d = string.split(f,'.')[0]
    copyfile(os.path.join(srcdir, 'doc', f),
             os.path.join(dstdir, 'doc', 'MonetDB', 'TechDocs', 'Core', d, f))

for f in ['bat.png', 'bat1.png', 'bat2.png']:
    copyfile(os.path.join(srcdir, 'src', 'gdk', f),
             os.path.join(tmpdir, f))
for f in ['gdk', 'gdk_atoms']:
    runMxTexi(os.path.join(srcdir, 'src', 'gdk'), f,
              os.path.join(dstdir, 'doc', 'MonetDB', 'TechDocs', 'Core'))
for f in ['bat.png', 'bat1.png', 'bat2.png']:
    copyfile(os.path.join(srcdir, 'src', 'gdk', f),
             os.path.join(dstdir, 'doc', 'MonetDB', 'TechDocs', 'Core', 'gdk', f))

runMx(os.path.join(srcdir, 'src', 'monet'), 'monet',
      os.path.join(dstdir, 'doc', 'MonetDB'))
copyfile(os.path.join(srcdir, 'src', 'monet', 'monet.gif'),
         os.path.join(dstdir, 'doc', 'MonetDB', 'monet', 'monet.gif'))

runMx(os.path.join(srcdir, 'src', 'mapi', 'clients', 'C'), 'Mapi',
      os.path.join(dstdir, 'doc', 'MonetDB', 'TechDocs', 'APIs'), os.sep + 'C')

runMx(os.path.join(srcdir, 'src', 'mapi', 'clients', 'C'), 'MapiClient',
      os.path.join(dstdir, 'doc', 'MonetDB', 'TechDocs', 'Tools'))

runMx(os.path.join(srcdir, 'src', 'tools'), 'Mserver',
      os.path.join(dstdir, 'doc', 'MonetDB', 'TechDocs', 'Tools'))

runMx(os.path.join(srcdir, 'src', 'mapi'), 'mapi',
      os.path.join(dstdir, 'doc', 'MonetDB', 'TechDocs', 'Core', 'Modules'))

runMx(os.path.join(srcdir, 'src', 'modules', 'calibrator'), 'calib',
      os.path.join(dstdir, 'doc', 'MonetDB', 'TechDocs', 'Core', 'Modules'))

for f in ['aggrX3', 'aggr', 'alarm', 'algebra', 'arith', 'ascii_io', 'bat',
          'blob', 'counters', 'decimal', 'enum', 'builtin', 'lock',
          'logger', 'mmath', 'monettime', 'pcl', 'pcre', 'radix',
          'streams', 'str', 'sys', 'tcpip', 'trans', 'unix',
          'upgrade', 'url', 'xtables']:
    runMx(os.path.join(srcdir, 'src', 'modules', 'plain'), f,
          os.path.join(dstdir, 'doc', 'MonetDB', 'TechDocs', 'Core', 'Modules'))

for f in ['bitset', 'bitvector', 'ddbench', 'mel', 'mprof', 'oo7', 'qt',
          'wisc']:
    runMx(os.path.join(srcdir, 'src', 'modules', 'contrib'), f,
          os.path.join(dstdir, 'doc', 'MonetDB', 'TechDocs', 'Core', 'Modules'))

for f in ['README', 'load.mil', 'init.mil']:
    copyfile(os.path.join(srcdir, 'scripts', 'gold', f),
             os.path.join(dstdir, 'doc', 'MonetDB', f))
for f in ['HowToStart', 'HowToStart-Win32.txt']:
    copyfile(os.path.join(srcdir, f),
             os.path.join(dstdir, 'doc', 'MonetDB', f))
os.makedirs(os.path.join(dstdir, 'doc', 'MonetDB', 'GetGoing', 'Setup', 'MonetDB', 'Unix'))
copyfile(os.path.join(srcdir, 'HowToStart'),
             os.path.join(dstdir, 'doc', 'MonetDB', 'GetGoing', 'Setup', 'MonetDB', 'Unix', 'index.html'))
os.makedirs(os.path.join(dstdir, 'doc', 'MonetDB', 'GetGoing', 'Setup', 'MonetDB', 'Windows'))
copyfile(os.path.join(srcdir, 'HowToStart-Win32.txt'),
             os.path.join(dstdir, 'doc', 'MonetDB', 'GetGoing', 'Setup', 'MonetDB', 'Windows', 'index.html'))

os.makedirs(os.path.join(dstdir, 'doc', 'MonetDB', 'monet-compiled', 'etc'))
copyfile(os.path.join(blddir, 'conf', 'MonetDB.conf'),
             os.path.join(dstdir, 'doc', 'MonetDB', 'monet-compiled', 'etc', 'MonetDB.conf'))
os.makedirs(os.path.join(dstdir, 'doc', 'MonetDB', 'monet-compiled', 'share', 'MonetDB', 'docs', 'gdk'))
##copyfile(os.path.join(blddir, 'src', 'gdk', 'gdk_atoms.html'),
##             os.path.join(dstdir, 'doc', 'MonetDB', 'monet-compiled', 'share', 'MonetDB', 'docs', 'gdk', 'gdk_atoms.html'))

f = open(os.path.join(dstdir, 'doc', 'MonetDB', 'sql.html'), 'w')
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
