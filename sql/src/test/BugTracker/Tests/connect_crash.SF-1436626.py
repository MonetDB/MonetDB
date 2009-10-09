import subprocess, os, time

def main():
    srvcmd = '%s --dbname "%s" --dbinit "include sql;"' % (os.getenv('MSERVER'),os.getenv('TSTDB'))
    srv = subprocess.Popen(srvcmd, shell = True, stdin = subprocess.PIPE)
    time.sleep(10)                      # give server time to start
    cltcmd = os.getenv('SQL_CLIENT')
    clt = subprocess.Popen(cltcmd, shell = True, stdin = subprocess.PIPE)
    clt.stdin.write('select 1;\n')
    clt.communicate()
    srv.communicate()

main()
