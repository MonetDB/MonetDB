import os, sys, zipfile

dbfarm = os.environ['GDK_DBFARM']
db = os.path.join(dbfarm, os.environ['TSTDB'])
archive = os.path.join(dbfarm, 'prevhgerelempty.zip')
if not os.path.exists(archive):
    sys.exit(1)

z = zipfile.ZipFile(archive)
z.extractall(db)
z.close()
