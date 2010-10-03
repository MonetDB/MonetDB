import sys
import os
from MonetDBtesting import process

def server():
    s = process.server('sql', args = ["--set", "gdk_readonly=yes"],
                       stdin = process.PIPE,
                       stdout = process.PIPE,
                       stderr = process.PIPE)
    s.stdin.write('\nio.printf("\\nReady.\\n");\n')
    s.stdin.flush()
    while True:
        ln = s.stdout.readline()
        if not ln:
            print 'Unexpected EOF from server'
            sys.exit(1)
        sys.stdout.write(ln)
        if 'Ready' in ln:
            break
    return s

def server_stop(s):
    out, err = s.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)

def client(input):
    c = process.client('sql',
                         stdin = process.PIPE,
                         stdout = process.PIPE,
                         stderr = process.PIPE)
    out, err = c.communicate(input)
    sys.stdout.write(out)
    sys.stderr.write(err)

script1 = '''\
create table t2 (a int);
'''

def main():
    s = server()
    client(script1)
    server_stop(s)

if __name__ == '__main__':
    main()
