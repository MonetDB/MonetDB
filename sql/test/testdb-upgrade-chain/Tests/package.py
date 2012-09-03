import os, sys, zipfile

dbfarm = os.environ['GDK_DBFARM']
db = os.path.join(dbfarm, os.environ['TSTDB'])
archive = os.path.join(dbfarm, 'prevchainrel.zip')
rev = os.getenv('REVISION')

if not os.path.exists(db):
    print >> sys.stderr, 'database directory %s does not exist' % db
    sys.exit(1)

f = open(os.path.join(db, 'bat', 'BACKUP', 'BBP.dir'), 'rU')
hdr = f.readline()
ptroid = f.readline()
ptr, oid = ptroid.split()
f.close()

z = zipfile.ZipFile(archive)
comment = z.comment
z.close()

try:
    # try to create compressed zip file
    z = zipfile.ZipFile(os.path.join(dbfarm, 'newchainrel.zip'), 'w',
                        zipfile.ZIP_DEFLATED)
except RuntimeError:
    # if that fails, create uncompressed zip file
    z = zipfile.ZipFile(os.path.join(dbfarm, 'newchainrel.zip'), 'w')

if rev:
    revcomment = ' (hg id %s)' % rev
else:
    revcomment = ''
z.comment = comment + 'Chained on host %s%s.\n' % (os.getenv('HOSTNAME', 'unknown'), revcomment)

for root, dirs, files in os.walk(db):
    for f in files:
        ff = os.path.join(root, f)
        z.write(ff, ff[len(db) + len(os.sep):].replace(os.sep, '/'))

z.close()
