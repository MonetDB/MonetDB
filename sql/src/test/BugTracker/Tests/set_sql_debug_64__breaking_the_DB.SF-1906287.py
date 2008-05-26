import os, time, sys

def clean_ports(cmd,mapiport,xrpcport):
    cmd = cmd.replace('--port=%s' % mapiport,'--port=<mapi_port>')
    cmd = cmd.replace('--set mapi_port=%s' % mapiport,'--set mapi_port=<mapi_port>')
    cmd = cmd.replace('--set xrpc_port=%s' % xrpcport,'--set xrpc_port=<xrpc_port>')
    return cmd

def server_start(debug,dbinit):
    port = int(os.getenv('MAPIPORT'))
    srvcmd = '%s --dbfarm "%s" --dbname "%s" --dbinit="%s" %s' % (os.getenv('MSERVER'),os.getenv('GDK_DBFARM'), os.getenv('TSTDB'),dbinit,debug)
    srvcmd_ = clean_ports(srvcmd,str(port),os.getenv('XRPCPORT'))
    sys.stderr.write('#mserver: "%s"\n' % (srvcmd))
    sys.stderr.flush()
    srv = os.popen(srvcmd, 'w')
    time.sleep(5)                      # give server time to start
    return srv

def server_stop(srv):
    srv.close()

def client_load_file(clt, port, file):
    f = open(file, 'r')
    for line in f:
        clt.write(line)
    f.close()


def client(lang, file):
    cltcmd = '%s' % os.getenv('%s_CLIENT' % lang)
    cltcmd_ = clean_ports(cltcmd,os.getenv('MAPIPORT'),os.getenv('XRPCPORT'))
    sys.stderr.flush()
    sys.stderr.write('#client: "%s"\n' % (cltcmd))
    sys.stderr.flush()
    clt = os.popen(cltcmd, 'w')
    port = int(os.getenv('MAPIPORT'))
    client_load_file(clt, port, file)
    clt.close()
    return '%s ' % (lang)


def main():
    srv = server_start("--set sql_debug=64","include sql;")
    client('SQL' , '%s/set_sql_debug_64__breaking_the_DB.SF-1906287_create.sql' % os.getenv('RELSRCDIR'))
    server_stop(srv)
    srv = server_start("","include sql;")
    client('SQL' , '%s/set_sql_debug_64__breaking_the_DB.SF-1906287_drop.sql' % os.getenv('RELSRCDIR'))
    server_stop(srv)

main()
