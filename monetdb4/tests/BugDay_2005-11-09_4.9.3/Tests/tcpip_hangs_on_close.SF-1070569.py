import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

def server_start(dbname):
    if os.name == 'nt':
        bufsize = -1
    else:
        bufsize = 0
    return process.server('mil', dbname = dbname,
                          stdin = process.PIPE, stdout = process.PIPE,
                          bufsize = bufsize)

def server_stop(srv):
    out, err = srv.communicate("quit();\n")
    sys.stdout.write(out)

prelude_1 = '''
module(tcpip);
module(alarm);
VAR mapiport := monet_environment.find("mapi_port");
'''

script_1 = '''
{
fork(listen(int(mapiport)+%d));
var y := import("x", true);
y.print();
}
'''
#terminate(int(mapiport)+1);

prelude_2 = '''
module(tcpip);
module(alarm);
module(unix);
VAR host := getenv("HOST");
VAR mapiport := monet_environment.find("mapi_port");
'''

script_2 = '''
{
sleep(2); # waiting for first server to start listening
VAR c := open(host+":"+(int(mapiport)+%d));
var x := bat(oid, oid);
x.insert(0@0, 0@0);
x.insert(1@0, 0@0);
x.sort().print();
c.export(x, "x");
close(c);
}
'''

def main():
    x = 0
    x += 1; srv1 = server_start("db" + str(x))
    x += 1; srv2 = server_start("db" + str(x))

    srv1.stdin.write(prelude_1)
    srv2.stdin.write(prelude_2)
    i = 0
    while i < 4:
        srv1.stdin.write(script_1 % (i))
        srv2.stdin.write(script_2 % (i))
        i += 1

    server_stop(srv1)
    server_stop(srv2)

main()
