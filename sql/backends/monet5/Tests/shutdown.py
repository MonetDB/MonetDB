
import os
import shutil

databasedir = os.getenv('TSTTRGDIR')
if databasedir == None:
    exit()
databasedir = os.path.join(databasedir, 'SHUTDOWN_DATABASE')

try:
    if os.name == 'nt':
        os.system('shutdowntest.exe "%s"' % databasedir)
    else:
        os.system('shutdowntest "%s"' % databasedir)
finally:
    shutil.rmtree(databasedir)
