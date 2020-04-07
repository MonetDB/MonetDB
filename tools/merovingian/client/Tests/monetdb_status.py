try:
    from MonetDBtesting import process
except ImportError:
    # import process
    pass
import os, sys
import subprocess

dbfarm = os.getenv('GDK_DBFARM')
tstdb = os.getenv('TSTDB')

if not tstdb or not dbfarm:
    print('No TSTDB or GDK_DBFARM in environment')
    sys.exit(1)

cmd = "monetdb version"

response = subprocess.check_output(cmd, shell=True)

#sys.stdout.write(response.decode("utf-8"))
#sys.stderr.write(serr)
