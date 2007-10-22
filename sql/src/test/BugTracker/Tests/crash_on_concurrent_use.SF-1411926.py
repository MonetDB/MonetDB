import os, time, sys, subprocess, threading

def server_start(dbinit):
    srvcmd = '%s --dbname "%s" --dbinit="%s"' % (os.getenv('MSERVER'),os.getenv('TSTDB'),dbinit)
    sys.stderr.write('#mserver: "%s"\n' % (srvcmd))
    sys.stdout.flush()
    sys.stderr.flush()
    srv = os.popen(srvcmd, 'w')
    time.sleep(5)                      # give server time to start
    return srv

def server_stop(srv):
    time.sleep(5)                      # give server time to start
    srv.close()

def clients(client, runs, clmd):
    for i in range(runs):
        sys.stdout.write('#start client %d run %d %d \t' % (client, i, time.time()))
        sys.stdout.flush()
        sys.stderr.flush()
        clt = os.popen(clcmd, 'w')
        clt.close()
        sys.stdout.write('#close client %d run %d %d \n' % (client, i, time.time()))


class Client(threading.Thread):
    def __init__(self, client):
        threading.Thread.__init__ (self)
        self.client= client

    def run(self):
        global clcmd
        clcmd = str(os.getenv('SQL_CLIENT')) + "< %s" % ('%s/crash_on_concurrent_use.SF-1411926.sql' % os.getenv('RELSRCDIR'))
        sys.stderr.write('#client%d: "%s"\n' % (self.client, clcmd))
        clients(self.client, 20, clcmd)

def main():
    check_version = os.system('%s --version' % os.getenv('MSERVER'))
    if check_version == 0:
        srv = server_start('include sql;')
    else:
        srv = server_start('module(sql_server);')
    client_0 = Client(0)
    client_1 = Client(1)
    client_0.start()
    client_1.start()
    server_stop(srv)

main()
