
import os
import shutil

bindir = os.getenv('TSTTRGBASE')
databasedir = os.getenv('TSTTRGDIR')
if databasedir == None:
    exit()
databasedir = os.path.join(databasedir, 'SHUTDOWN_DATABASE')
if bindir == None:
    os.system('shutdowntest "%s"' % databasedir)
else:
    os.system(os.path.join(bindir, 'bin', 'shutdowntest') + ' "%s"' % databasedir)

shutil.rmtree(databasedir)
