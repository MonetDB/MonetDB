import os, sys, zipfile

dbfarm = os.environ['GDK_DBFARM']
db = os.path.join(dbfarm, os.environ['TSTDB'])
rev = os.getenv('REVISION')

if not os.path.exists(db):
    print >> sys.stderr, 'database directory %s does not exist' % db
    sys.exit(1)

f = open(os.path.join(db, 'bat', 'BACKUP', 'BBP.dir'), 'rU')
hdr = f.readline()
ptroid = f.readline()
ptr, oid = ptroid.split()
f.close()

try:
    # try to create compressed zip file
    z = zipfile.ZipFile(os.path.join(dbfarm, 'newrel.zip'), 'w',
                        zipfile.ZIP_DEFLATED)
except RuntimeError:
    # if that fails, create uncompressed zip file
    z = zipfile.ZipFile(os.path.join(dbfarm, 'newrel.zip'), 'w')

if rev:
    revcomment = ' (hg id %s)' % rev
else:
    revcomment = ''
z.comment = 'TestDB created on host %s with pointer size %s and oid size %s%s.\n' % (os.getenv('HOSTNAME', 'unknown'), ptr, oid, revcomment)

for root, dirs, files in os.walk(db):
    for f in files:
        ff = os.path.join(root, f)
        z.write(ff, ff[len(db) + len(os.sep):].replace(os.sep, '/'))

z.close()
