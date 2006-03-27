
import os, time, sys

class Popen:
	def __init__(self, cmd):
		self.stdin,self.stdout = os.popen2(cmd, bufsize=0, mode='t'); 


def server_start(x,dbname):
    srvcmd = '%s --dbname "%s"' % (os.getenv('MSERVER'),dbname)
    return Popen(srvcmd);

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
module(alarm);
VAR mapiport := monet_environment.find("mapi_port");
sleep(2);	# waiting for first server to start listening
VAR c := open("localhost:"+mapiport);
'''

script_2 = '''
var x := bat(oid, oid);
x.insert(0@0, 0@0);
x.insert(1@0, 0@0);
x.sort().print();
c.export(x, "x");
close(c);
'''

script_1 = '''
var y := import("x", true);
y.print();
terminate(int(mapiport));
'''

def main():
    x = 0
    x += 1; srv1 = server_start(x, "db" + str(x))
    x += 1; srv2 = server_start(x, "db" + str(x))
    time.sleep(1)                      # give servers time to start

    i = 0
    while i < 4:
    	srv1.stdin.write(prelude_1)
    	srv2.stdin.write(prelude_2)
   
    	srv2.stdin.write(script_2)
    	srv1.stdin.write(script_1)
	i += 1
 
    srv1.stdin.write("quit();\n");
    srv2.stdin.write("quit();\n");

    server_stop(srv1);
    server_stop(srv2);

main()
