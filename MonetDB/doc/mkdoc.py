import sys, os

if len(sys.argv) != 3:
    print '''\
Usage: %s srcdir dstdir

where srcdir is the top of the MonetDB source directory tree, and
dstdir is the top of the installation tree.''' % sys.argv[0]
    sys.exit(1)

srcdir = sys.argv[1]
dstdir = sys.argv[2]
tmpdir = '/tmp'

def copy(srcfile, dstfile):
    print 'copy', srcfile, dstfile
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

def runMx(srcdir, base, tmpdir, instdir):
    print 'runMx', srcdir, base, tmpdir, instdir
    cmd = 'Mx "-R%s" -H1 -w "%s" 2>&1' % (tmpdir, os.path.join(srcdir, base + '.mx'))
    print cmd
    f = os.popen(cmd, 'r')
    dummy = f.read()                    # discard output
    f.close()
    srcfile = os.path.join(tmpdir, base + '.body.html')
    if not os.path.exists(srcfile):
        srcfile = os.path.join(tmpdir, base + '.html')
    copy(srcfile, os.path.join(instdir, base + '.html'))
    unlink(os.path.join(tmpdir, base + '.html'))
    unlink(os.path.join(tmpdir, base + '.index.html'))
    unlink(os.path.join(tmpdir, base + '.body.html'))

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
os.makedirs(os.path.join(dstdir, 'doc', 'www'))

for f in ['monet.gif', 'mel.gif']:
    copy(os.path.join(srcdir, 'doc', f),
         os.path.join(dstdir, 'doc', f))
for f in ['bat.gif', 'bat1.gif', 'bat2.gif']:
    copy(os.path.join(srcdir, 'src', 'gdk', f),
         os.path.join(dstdir, 'doc', 'www', f))

for f in ['monet', 'mil', 'mel']:
    runMx(os.path.join(srcdir, 'doc'), f, tmpdir, os.path.join(dstdir, 'doc'))

runMx(os.path.join(srcdir, 'src', 'gdk'), 'gdk', tmpdir, os.path.join(dstdir, 'doc', 'www'))
runMx(os.path.join(srcdir, 'src', 'gdk'), 'gdk_atoms', tmpdir, os.path.join(dstdir, 'doc', 'www'))

runMx(os.path.join(srcdir, 'src', 'monet'), 'monet', tmpdir, os.path.join(dstdir, 'doc', 'www'))

runMx(os.path.join(srcdir, 'src', 'mapi', 'clients'), 'MapiClient', tmpdir, os.path.join(dstdir, 'doc', 'www'))

runMx(os.path.join(srcdir, 'src', 'tools'), 'Mserver', tmpdir, os.path.join(dstdir, 'doc', 'www'))

runMx(os.path.join(srcdir, 'src', 'mapi'), 'mapi', tmpdir, os.path.join(dstdir, 'doc', 'www'))

runMx(os.path.join(srcdir, 'src', 'modules', 'calibrator'), 'calib', tmpdir, os.path.join(dstdir, 'doc', 'www'))

for f in ['aggrX3', 'aggr', 'alarm', 'algebra', 'arith', 'ascii_io', 'bat',
          'bitset', 'bitvector', 'blob', 'counters', 'ddbench', 'decimal',
          'enum', 'kernel', 'lock', 'logger', 'mel', 'mmath', 'monettime',
          'mprof', 'oo7', 'qt', 'radix', 'streams', 'str', 'sys', 'tcpip',
          'tpcd', 'trans', 'unix', 'url', 'wisc', 'xtables']:
    runMx(os.path.join(srcdir, 'src', 'modules', 'plain'), f, tmpdir, os.path.join(dstdir, 'doc', 'www'))

for f in ['README', 'init.mil']:
    copy(os.path.join(srcdir, 'scripts', 'gold', f),
         os.path.join(dstdir, 'doc', 'www', f))
copy(os.path.join(srcdir, 'HowToStart'),
     os.path.join(dstdir, 'doc', 'www', 'HowToStart'))

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
