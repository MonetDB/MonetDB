import os, time
try:
    import subprocess
except ImportError:
    # user private copy for old Python versions
    import MonetDBtesting.subprocess26 as subprocess

def main():
    srvcmd = '%s --set mapi_port=%s --dbinit "module(pathfinder);module(sql_server);mil_start();"' % (os.getenv('MSERVER'),os.getenv('MAPIPORT'))
    srv = subprocess.Popen(srvcmd, shell = True, stdin = subprocess.PIPE)
    time.sleep(10)                      # give server time to start
    cltcmd = os.getenv('MIL_CLIENT')
    clt = subprocess.Popen(cltcmd, shell = True, stdin = subprocess.PIPE)
    clt.communicate()
    srv.communicate()

main()
