import os, time, sys

def clean_ports(cmd,mapiport,xrpcport):
    cmd = cmd.replace('--port=%s' % mapiport,'--port=<mapi_port>')
    cmd = cmd.replace('--set mapi_port=%s' % mapiport,'--set mapi_port=<mapi_port>')
    cmd = cmd.replace('--set xrpc_port=%s' % xrpcport,'--set xrpc_port=<xrpc_port>')
    return cmd

def clean_login(cmd,user,passwd):
    cmd = cmd.replace('-umonetdb','-u%s' % user)
    cmd = cmd.replace('-Pmonetdb','-P%s' % passwd)
    return cmd

def server_start(dbinit):
    port = int(os.getenv('MAPIPORT'))
    srvcmd = '%s --dbfarm "%s" --dbname "%s" --dbinit="%s"' % (os.getenv('MSERVER'),os.getenv('GDK_DBFARM'), os.getenv('TSTDB'),dbinit)
    srvcmd_ = clean_ports(srvcmd,str(port),os.getenv('XRPCPORT'))
    sys.stderr.write('#mserver: "%s"\n' % (srvcmd))
    sys.stderr.flush()
    srv = os.popen(srvcmd, 'w')
    time.sleep(2)                      # give server time to start
    return srv

def server_stop(srv):
    srv.close()
    time.sleep(2)                      # give server time to stop

def client_load_file(clt, port, file):
    f = open(file, 'r')
    for line in f:
        clt.write(line)
    f.close()


def client(lang, file, user, passwd):
    cltcmd = '%s' % os.getenv('%s_CLIENT' % lang)
    cltcmd_ = clean_ports(cltcmd,os.getenv('MAPIPORT'),os.getenv('XRPCPORT'))
    cltcmd_ = clean_login(cltcmd,user, passwd)
    sys.stderr.flush()
    sys.stderr.write('#client: "%s"\n' % (cltcmd_))
    sys.stderr.flush()
    clt = os.popen(cltcmd, 'w')
    port = int(os.getenv('MAPIPORT'))
    client_load_file(clt, port, file)
    clt.close()
    return '%s ' % (lang)


def main():
    dbinit = "include sql;"

    srv = server_start(dbinit)
    client('SQL' , '%s/set_a_new_user_password.SF-1844050_create_user.sql' % os.getenv('RELSRCDIR'), "monetdb", "monetdb")
    server_stop(srv)

    srv = server_start(dbinit)
    client('SQL' , '%s/set_a_new_user_password.SF-1844050_select.sql' % os.getenv('RELSRCDIR'), "voc2", "new")
    server_stop(srv)

    srv = server_start(dbinit)
    client('SQL' , '%s/set_a_new_user_password.SF-1844050_drop_user.sql' % os.getenv('RELSRCDIR'), "monetdb", "monetdb")
    server_stop(srv)

main()
