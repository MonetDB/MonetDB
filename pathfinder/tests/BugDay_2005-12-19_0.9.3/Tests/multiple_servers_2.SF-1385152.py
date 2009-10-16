import os, time, sys
from MonetDBtesting import process

def server_start(x,s,dbinit):
    sys.stdout.write('\nserver %d%d : "%s"\n' % (x,s,dbinit))
    sys.stderr.write('\nserver %d%d : "%s"\n' % (x,s,dbinit))
    sys.stdout.flush()
    sys.stderr.flush()
    srv = process.server('mil', dbinit = dbinit, stdin = process.PIPE)
    time.sleep(5)                      # give server time to start
    return srv

def server_stop(srv):
    srv.communicate()

def client(x,s, c, dbinit, lang, cmd, h):
    sys.stdout.write('\nserver %d%d : "%s", client %d: %s%s\n' % (x,s,dbinit,c,h,lang))
    sys.stderr.write('\nserver %d%d : "%s", client %d: %s%s\n' % (x,s,dbinit,c,h,lang))
    sys.stdout.flush()
    sys.stderr.flush()
    clt = process.client(lang, stdin = process.PIPE)
    clt.communicate(cmd)

def clients(x,dbinit):
    s = 0
    s += 1; srv = server_start(x,s,dbinit)
    c = 0 ;
    c += 1; client(x,s,c,dbinit,'xquery',      '%d%d%d\n'   % (x,s,c),h)
    c += 1; client(x,s,c,dbinit,'xquery',      '%d%d%d\n'   % (x,s,c),h)
    c += 1; client(x,s,c,dbinit,'mil'   ,'print(%d%d%d);\n' % (x,s,c),h)
    c += 1; client(x,s,c,dbinit,'mil'   ,'print(%d%d%d);\n' % (x,s,c),h)
    server_stop(srv)
    s += 1; srv = server_start(x,s,dbinit)
    c = 0 ;
    c += 1; client(x,s,c,dbinit,'xquery',      '%d%d%d\n'   % (x,s,c),h)
    c += 1; client(x,s,c,dbinit,'mil'   ,'print(%d%d%d);\n' % (x,s,c),h)
    c += 1; client(x,s,c,dbinit,'xquery',      '%d%d%d\n'   % (x,s,c),h)
    c += 1; client(x,s,c,dbinit,'mil'   ,'print(%d%d%d);\n' % (x,s,c),h)
    server_stop(srv)
    s += 1; srv = server_start(x,s,dbinit)
    c = 0 ;
    c += 1; client(x,s,c,dbinit,'mil'   ,'print(%d%d%d);\n' % (x,s,c),h)
    c += 1; client(x,s,c,dbinit,'xquery',      '%d%d%d\n'   % (x,s,c),h)
    c += 1; client(x,s,c,dbinit,'xquery',      '%d%d%d\n'   % (x,s,c),h)
    c += 1; client(x,s,c,dbinit,'mil'   ,'print(%d%d%d);\n' % (x,s,c),h)
    server_stop(srv)
    s += 1; srv = server_start(x,s,dbinit)
    c = 0 ;
    c += 1; client(x,s,c,dbinit,'xquery',      '%d%d%d\n'   % (x,s,c),h)
    c += 1; client(x,s,c,dbinit,'mil'   ,'print(%d%d%d);\n' % (x,s,c),h)
    c += 1; client(x,s,c,dbinit,'mil'   ,'print(%d%d%d);\n' % (x,s,c),h)
    c += 1; client(x,s,c,dbinit,'xquery',      '%d%d%d\n'   % (x,s,c),h)
    server_stop(srv)
    s += 1; srv = server_start(x,s,dbinit)
    c = 0 ;
    c += 1; client(x,s,c,dbinit,'mil'   ,'print(%d%d%d);\n' % (x,s,c),h)
    c += 1; client(x,s,c,dbinit,'xquery',      '%d%d%d\n'   % (x,s,c),h)
    c += 1; client(x,s,c,dbinit,'mil'   ,'print(%d%d%d);\n' % (x,s,c),h)
    c += 1; client(x,s,c,dbinit,'xquery',      '%d%d%d\n'   % (x,s,c),h)
    server_stop(srv)
    s += 1; srv = server_start(x,s,dbinit)
    c = 0 ;
    c += 1; client(x,s,c,dbinit,'mil'   ,'print(%d%d%d);\n' % (x,s,c),h)
    c += 1; client(x,s,c,dbinit,'mil'   ,'print(%d%d%d);\n' % (x,s,c),h)
    c += 1; client(x,s,c,dbinit,'xquery',      '%d%d%d\n'   % (x,s,c),h)
    c += 1; client(x,s,c,dbinit,'xquery',      '%d%d%d\n'   % (x,s,c),h)
    server_stop(srv)

def main():
    x = 0
    x += 1; clients(x,"module(pathfinder); mil_start();")

main()
