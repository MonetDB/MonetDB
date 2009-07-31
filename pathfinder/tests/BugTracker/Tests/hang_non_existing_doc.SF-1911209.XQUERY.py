import os, sys
try:
    import subprocess
except ImportError:
    # user private copy for old Python versions
    import MonetDBtesting.subprocess26 as subprocess

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
    # HACK ALERT: create updatable document by appending ,10 to collection name
    client(xq_client + ['--input=doc1911209.xml', '--collection=doc1911209.xml,10'],
           '<aap/>')
    client(xq_client + ['-s', 'do insert <beer/> into doc("doc1911209.xml")/aap'])
    for i in range(1000):
        client(xq_client + ['-s', 'pf:documents()'])
        client(xq_client + ['-s', 'doc("does_not_exist.xml")'])
    client(xq_client + ['-s', 'pf:del-doc("doc1911209.xml")'])

main()
