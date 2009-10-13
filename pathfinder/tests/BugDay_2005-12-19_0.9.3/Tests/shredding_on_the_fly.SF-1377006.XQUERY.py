import os
import subprocess

def main():
    fn = os.path.join(os.getenv('TSTSRCDIR'), 'test1377006.xml')
    p = subprocess.Popen(os.getenv('XQUERY_CLIENT'), shell = True, stdin = subprocess.PIPE)
    p.communicate('doc("%s")/x' % fn)

main()
