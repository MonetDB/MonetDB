import os, time, sys

def server_start(x,s,dbinit):
    port = int(os.getenv('MAPIPORT'))
    srvcmd = '%s --dbname "%s_test1" --set mapi_port=%d --dbinit="%s"' % (os.getenv('MSERVER'),os.getenv('TSTDB'),(port+1),dbinit)
    sys.stdout.write('\nserver %d%d : "%s"\n' % (x,s, dbinit))
    sys.stderr.write('\nserver %d%d : "%s"\n' % (x,s, dbinit))
    sys.stderr.flush()
    sys.stderr.write('\nmserver: "%s"\n' % (srvcmd))
    sys.stdout.flush()
    sys.stderr.flush()
    srv = os.popen(srvcmd, 'w')
    time.sleep(5)                      # give server time to start
    return srv

def server_stop(srv):
    srv.close()

def client(x,s, c, dbinit, lang, file):
    cltcmd = '%s < %s/../%s' % (os.getenv('%s_CLIENT' % lang),os.getenv('RELSRCDIR'),file)
    sys.stdout.write('\nserver %d%d : "%s", client %d: %s\n' % (x,s,dbinit,c,lang))
    sys.stderr.write('\nserver %d%d : "%s", client %d: %s\n' % (x,s,dbinit,c,lang))
    sys.stderr.flush()
    sys.stderr.write('\nclient: "%s"\n' % (cltcmd))
    sys.stdout.flush()
    sys.stderr.flush()
    clt = os.popen(cltcmd, 'w')
    clt.close()
    return '%s ' % (lang)

def clients(x,dbinit):
    s = 0
    s += 1; srv = server_start(x,s,dbinit)
    c = 0 ; h = ''
    c += 1; h = client(x,s,c,dbinit,'SQL' , 'connections_syntax.sql')
    c += 1; h = client(x,s,c,dbinit,'SQL' , 'connections_semantic.sql')
    c += 1; h = client(x,s,c,dbinit,'SQL', 'connections_default_values.sql')
    server_stop(srv)

def main():
    x = 0
    x += 1; clients(x,'include sql;')

main()
