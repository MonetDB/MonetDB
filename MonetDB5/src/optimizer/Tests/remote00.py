
import os, time, sys, re

class Popen:
    def __init__(self, cmd):
        self.stdin,self.stdout = os.popen2(cmd);

def server_start(x,dbname,mapi_port):
    srvcmd = '%s --debug=10 --dbname "%s"' % (re.sub('mapi_port=[^ ]* ','mapi_port=%d ' % mapi_port,os.getenv('MSERVER')),dbname)
    return Popen(srvcmd);

def server_stop(srv):
    srv.stdin.close()
    r = srv.stdout.read()
    sys.stdout.write(r);
    srv.stdout.close()

prelude_1 = '''
io.print(1);
env := inspect.getEnvironment();
mapi_port := algebra.find(env, "mapi_port");
io.print(mapi_port);
alarm.sleep(4);
'''

# The last part of server 2 is on one line to make sure the optimizer is called
prelude_2 = '''
alarm.sleep(2);
env := inspect.getEnvironment();
mapi_port := algebra.find(env, "mapi_port");
io.print(mapi_port);

mid:= mapi.reconnect("localhost",%d,"s0_0","monetdb","monetdb","mal"); mapi.rpc(mid,"rb:= bat.new(:int,:int); bat.setName(rb,\\\"rbat\\\"); bat.insert(rb,1,1); bat.insert(rb,2,7);"); b:bat[:int,:int]:= mapi.bind(mid,"rbat"); c:=algebra.select(b,0,12); io.print(c); d:=algebra.select(b,5,10); low:= 5+1; e:=algebra.select(d,low,7); i:=aggr.count(e); io.print(i); io.print(d); optimizer.remoteQueries();
'''

def main():
    x = 0
    x += 1; srv1 = server_start(x, "db" + str(x), 12345)
    x += 1; srv2 = server_start(x, "db" + str(x), 12346)

    srv1.stdin.write(prelude_1)
    srv1.stdin.flush()
    srv2.stdin.write(prelude_2 % (12345))
    srv2.stdin.flush()

    srv1.stdin.write("clients.quit();\n");
    srv1.stdin.flush()
    srv2.stdin.write("clients.quit();\n");
    srv2.stdin.flush()

    server_stop(srv1);
    server_stop(srv2);

main()
