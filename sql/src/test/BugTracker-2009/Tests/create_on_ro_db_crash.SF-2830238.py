import sys
import os
import time
import subprocess

def server():
    s = subprocess.Popen('%s "--dbinit=include sql;" --set gdk_readonly=yes' % os.getenv('MSERVER'),
                         shell = True,
                         stdin = subprocess.PIPE,
                         stdout = subprocess.PIPE,
                         stderr = subprocess.PIPE)
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

def client():
    c = subprocess.Popen("%s" % os.getenv('SQL_CLIENT'),
                         shell = True,
                         stdin = subprocess.PIPE,
                         stdout = subprocess.PIPE,
                         stderr = subprocess.PIPE)
    return c

script1 = '''\
create table t2 (a int);
'''

def main():
    s = server()
    c = client()
    o, e = c.communicate(script1)
    sys.stdout.write(o)
    sys.stderr.write(e)
    o, e = s.communicate()
    sys.stdout.write(o)
    sys.stderr.write(e)

if __name__ == '__main__':
    main()
