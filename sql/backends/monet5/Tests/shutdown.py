
import os
import shutil

databasedir = os.getenv('TSTTRGDIR')
if databasedir is None:
    exit("TSTTRGDIR needs to be in the environment")
databasedir = os.path.join(databasedir, 'SHUTDOWN_DATABASE')

try:
    os.system(f"shutdowntest{'.exe' if os.name == 'nt' else ''}"
              f" {'--pipeline' if os.getenv('PIPELINE') else ''}"
              f" {databasedir}")
finally:
    shutil.rmtree(databasedir)
