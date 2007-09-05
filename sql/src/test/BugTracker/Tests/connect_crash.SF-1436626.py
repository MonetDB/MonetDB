import os, time

def main():
    srvcmd = '%s --dbname "%s" --dbinit "module(sql_server); mil_start();"' % (os.getenv('MSERVER'),os.getenv('TSTDB'))
    srv = os.popen(srvcmd, 'w')
    time.sleep(10)                      # give server time to start
    cltcmd = os.getenv('SQL_CLIENT')
    clt = os.popen(cltcmd, 'w')
    clt.write('select 1;\n')
    clt.close()
    srv.close()

main()
