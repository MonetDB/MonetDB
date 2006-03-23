
import os, time, sys, subprocess 

PIPE = subprocess.PIPE

def server_start(x,dbname):
    srvcmd = '%s --dbname "%s"' % (os.getenv('MSERVER'),dbname)
    return subprocess.Popen(srvcmd, shell=True, bufsize=0, stdin=PIPE, stdout=PIPE);

def server_stop(srv):
    r = srv.stdout.read()
    sys.stdout.write(r);
    srv.stdout.close()
    srv.stdin.close()

prelude_1 = '''
module(tcpip);
VAR mapiport := monet_environment.find("mapi_port");
fork(listen(int(mapiport)));
'''

prelude_2 = '''
module(tcpip);
module(unix);
module(ascii_io);
VAR mapiport := monet_environment.find("mapi_port");
VAR c := open("localhost:"+mapiport);
VAR TSTTRGDIR := getenv("TSTTRGDIR");
'''

script_2 = '''
var test1 := bat(oid, oid);
test1.import(TSTTRGDIR + "/tcpip2.init.bat");
test1.kunique().count().print();
c.export(test1, "test1");
'''

script_1 = '''
var test2 := import("test1", true);
test2.kunique().count().print();
'''

def main():
    x = 0
    x += 1; srv1 = server_start(x, "db" + str(x))
    x += 1; srv2 = server_start(x, "db" + str(x))

    srv1.stdin.write(prelude_1)
    srv1.stdin.flush()
    time.sleep(1)                      # give server 1 time to start
    srv2.stdin.write(prelude_2)
    srv2.stdin.flush()
   
    srv2.stdin.write(script_2)
    srv2.stdin.flush()
    srv1.stdin.write(script_1)
    srv1.stdin.flush()
 
    srv1.stdin.write("quit();\n");
    srv1.stdin.flush()
    srv2.stdin.write("quit();\n");
    srv2.stdin.flush()

    server_stop(srv1);
    server_stop(srv2);

main()
