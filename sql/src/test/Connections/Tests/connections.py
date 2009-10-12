import os, time, sys
import subprocess

def remote_server_start(x,s,dbinit):
    port = os.getenv('MAPIPORT')
    srvcmd = '%s --dbfarm "%s" --dbname "%s_test1" --dbinit="%s"' % (os.getenv('MSERVER'),os.getenv('GDK_DBFARM'), os.getenv('TSTDB'),dbinit)
    srvcmd = srvcmd.replace(port,str(int(port)+1))
    sys.stdout.write('\nserver %d%d : "%s"\n' % (x,s, dbinit))
    sys.stderr.write('\nserver %d%d : "%s"\n' % (x,s, dbinit))
    sys.stderr.flush()
    sys.stderr.write('#remote mserver: "%s"\n' % (srvcmd))
    sys.stdout.flush()
    sys.stderr.flush()
    srv = subprocess.Popen(srvcmd, shell = True, stdin = subprocess.PIPE)
    time.sleep(5)                      # give server time to start
    return srv

def server_start(x,s,dbinit):
    port = int(os.getenv('MAPIPORT'))
    srvcmd = '%s --dbfarm "%s" --dbname "%s" --dbinit="%s"' % (os.getenv('MSERVER'),os.getenv('GDK_DBFARM'), os.getenv('TSTDB'),dbinit)
    sys.stdout.write('\nserver %d%d : "%s"\n' % (x,s, dbinit))
    sys.stderr.write('\nserver %d%d : "%s"\n' % (x,s, dbinit))
    sys.stderr.flush()
    sys.stderr.write('#mserver: "%s"\n' % (srvcmd))
    sys.stdout.flush()
    sys.stderr.flush()
    srv = subprocess.Popen(srvcmd, shell = True, stdin = subprocess.PIPE)
    time.sleep(5)                      # give server time to start
    return srv

def server_stop(srv):
    srv.communicate()

def client_load_file(clt, port, file):
    f = open(file, 'r')
    for line in f:
        line = line.replace('port_num5', str(port+2))
        line = line.replace('port_num', str(port+1))
        clt.stdin.write(line)
    f.close()


def client(x,s, c, dbinit, lang, file):
    cltcmd = '%s' % os.getenv('%s_CLIENT' % lang)
    sys.stdout.write('\nserver %d%d : "%s", client %d: %s\n' % (x,s,dbinit,c,lang))
    sys.stderr.write('\nserver %d%d : "%s", client %d: %s\n' % (x,s,dbinit,c,lang))
    sys.stderr.flush()
    sys.stderr.write('#client%d: "%s"\n' % (x,cltcmd))
    sys.stdout.flush()
    sys.stderr.flush()
    clt = subprocess.Popen(cltcmd, shell = True, stdin = subprocess.PIPE)
    port = int(os.getenv('MAPIPORT'))
    client_load_file(clt, port, file)
    clt.communicate()
    return '%s ' % (lang)


def clients(x,dbinit):
    s = 0
    s += 1; srv = server_start(x,s,dbinit)
    s += 1; remote_srv = remote_server_start(x,s,dbinit)
    c = 0 ; h = ''
    c += 1; h = client(x,s,c,dbinit,'SQL' , os.path.join(os.getenv('RELSRCDIR'),'..','connections_syntax.sql'))
    c += 1; h = client(x,s,c,dbinit,'SQL' , os.path.join(os.getenv('RELSRCDIR'),'..','connections_semantic.sql'))
    c += 1; h = client(x,s,c,dbinit,'SQL', os.path.join(os.getenv('RELSRCDIR'),'..','connections_default_values.sql'))
    server_stop(remote_srv)
    server_stop(srv)

def main():
    x = 0
    x += 1; clients(x,'include sql;')

main()
