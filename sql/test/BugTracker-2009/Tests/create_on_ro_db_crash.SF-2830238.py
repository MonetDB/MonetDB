import sys
import os
try:
    from MonetDBtesting import process
except ImportError:
    import process

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
    s = process.server(args = ["--set", "gdk_readonly=yes"],
                       stdin = process.PIPE,
                       stdout = process.PIPE,
                       stderr = process.PIPE)
    client(script1)
    out, err = s.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)

if __name__ == '__main__':
    main()
