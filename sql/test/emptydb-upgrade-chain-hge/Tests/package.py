from __future__ import print_function

import os, sys, zipfile

dbfarm = os.environ['GDK_DBFARM']
db = os.path.join(dbfarm, os.environ['TSTDB'])
archive = os.path.join(dbfarm, 'prevhgechainrelempty.zip')
rev = os.getenv('REVISION')

if not os.path.exists(db):
    print('database directory %s does not exist' % db, file=sys.stderr)
    sys.exit(1)

try:
    f = open(os.path.join(db, 'bat', 'BACKUP', 'SUBCOMMIT', 'BBP.dir'), 'rU')
except IOError:
    try:
        f = open(os.path.join(db, 'bat', 'BACKUP', 'BBP.dir'), 'rU')
    except IOError:
        f = open(os.path.join(db, 'bat', 'BBP.dir'), 'rU')
hdr = f.readline()
ptroid = f.readline()
ptr, oid, hge = ptroid.split()
f.close()

z = zipfile.ZipFile(archive)
comment = z.comment
z.close()

try:
    # try to create compressed zip file
    z = zipfile.ZipFile(os.path.join(dbfarm, 'newhgechainrelempty.zip'), 'w',
                        zipfile.ZIP_DEFLATED)
except RuntimeError:
    # if that fails, create uncompressed zip file
    z = zipfile.ZipFile(os.path.join(dbfarm, 'newhgechainrelempty.zip'), 'w')

if rev:
    revcomment = ' (hg id %s)' % rev
else:
    revcomment = ''
if hge == '16' and 'largest integer size 16' not in comment:
    revcomment = ' with largest integer size 16' + revcomment
z.comment = comment + 'Chained on host %s%s.\n' % (os.getenv('HOSTNAME', 'unknown'), revcomment)

for root, dirs, files in os.walk(db):
    for f in files:
        ff = os.path.join(root, f)
        z.write(ff, ff[len(db) + len(os.sep):].replace(os.sep, '/'))

z.close()
