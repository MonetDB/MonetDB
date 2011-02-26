import os, time, sys
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

PROC f(): int {
print(10);
RETURN 10;
}

fork(listen(int(mapiport)));
'''

prelude_2 = '''
module(tcpip);
module(unix);
VAR host := getenv("HOST");
VAR mapiport := monet_environment.find("mapi_port");
VAR c := open(host+":"+mapiport);
'''

script_2 = '''
var res := rpc(c, "f();"); # caused crash
print(res);
'''

script_1 = '''
sleep(2);
'''

def main():
    x = 0
    x += 1; srv1 = server_start("db" + str(x))
    x += 1; srv2 = server_start("db" + str(x))

    srv1.stdin.write(prelude_1)
    time.sleep(1)                      # give server 1 time to start
    srv2.stdin.write(prelude_2)

    srv2.stdin.write(script_2)
    srv1.stdin.write(script_1)

    server_stop(srv1)
    server_stop(srv2)

main()
