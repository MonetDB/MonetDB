#!/usr/bin/env python3

# When called in the test environment, this is called with exactly one
# argument: the name of the test.
# When called without argument, we dump the server output so that it
# can be saved to be tested against.
try:
    from MonetDBtesting import process
except ImportError:
    import process

import sys, os, zipfile, platform

xit = 0

# find zipped database
dbfarm = os.environ['GDK_DBFARM']
db = os.path.join(dbfarm, os.environ['TSTDB'])
archive = os.path.join(dbfarm, 'lastchainrel.zip')
if not os.path.exists(archive):
    print('file "{}" not found'.format(archive), file=sys.stderr)
    sys.exit(1)

# unpackage database
try:
    os.remove(os.path.join(db, '.vaultkey'))
except FileNotFoundError:
    pass
with zipfile.ZipFile(archive) as z:
    z.extractall(path=db)

# start server and dump database
with process.server(args=['--clean-BBP', '--set', 'allow_hge_upgrade=yes'],
                    mapiport='0',
                    stdin=process.PIPE,
                    stdout=process.PIPE,
                    stderr=process.PIPE) as srv:
    with process.client(lang='sql',
                        user='testuser',
                        passwd='testpassword',
                        stdin=process.PIPE,
                        stdout=process.PIPE,
                        stderr=process.PIPE,
                        server=srv) as clt:
        cltout, clterr = clt.communicate('select count(*) from testschema.smallstring;\n')
    srvout, srverr = srv.communicate()

srvout = [line for line in srvout.splitlines(keepends=True) if not line.startswith('#')]

# check server output (upgrade commands) and check dump (empty)
if len(sys.argv) == 2 and sys.argv[1] == 'upgrade':
    bits = os.getenv('TST_BITS')
    if bits:
        bits = ['.' + bits, '']
    else:
        bits = ['']
    arch = platform.machine()
    arch = ['.' + arch, '']
    hge = os.getenv('HAVE_HGE')
    if hge:
        hge = ['.int128', '']
    else:
        hge = ['']
    found = False
    for b in bits:
        for a in arch:
            for h in hge:
                f = 'upgrade.stable.out{}{}{}'.format(b, a, h)
                found = os.path.exists(f)
                if found:
                    break
            if found:
                break
        if found:
            break
    stable = open(f).readlines()
    if not os.getenv('HAVE_SHP'):
        for i in range(len(stable)):
            if 'create procedure SHPLoad' in stable[i]:
                del stable[i-1:i+4]
                break
    import difflib
    for line in difflib.unified_diff(stable, srvout, fromfile='test', tofile=f):
        sys.stderr.write(line)
        xit = 1
else:
    sys.stdout.writelines(srvout)

if xit:
    if clterr:
        sys.stderr.write('Client standard error:\n')
        sys.stderr.write(clterr)
        sys.stderr.write('\n')
    if srverr:
        sys.stderr.write('Server standard error:\n')
        sys.stderr.write(srverr)
        sys.stderr.write('\n')

sys.exit(xit)
