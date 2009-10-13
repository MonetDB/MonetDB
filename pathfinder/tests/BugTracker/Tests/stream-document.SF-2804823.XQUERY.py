import os, sys
import subprocess

def client(cmd, input = None):
    clt = subprocess.Popen(cmd,
                           stdin = subprocess.PIPE,
                           stdout = subprocess.PIPE,
                           stderr = subprocess.PIPE,
                           universal_newlines = True)
    out, err = clt.communicate(input)
    sys.stdout.write(out)
    sys.stderr.write(err)

def main():
    xq_client = os.getenv('XQUERY_CLIENT').split()
    client(xq_client + ['--input=my-document', '--collection=my-collection'],
           '<document>test document</document>')
    client(xq_client + ['-s', 'for $doc in pf:documents() where $doc/@url = "my-document" return $doc'])
    client(xq_client + ['-s', 'pf:del-doc("my-document")'])

main()
