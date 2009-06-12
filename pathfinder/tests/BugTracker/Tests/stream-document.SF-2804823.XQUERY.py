import os, sys
try:
    import sybprocess
except ImportError:
    # user private copy for old Python versions
    import MonetDBtesting.subprocess26 as subprocess

def client(cmd, input = None):
    clt = subprocess.Popen(cmd,
                           shell = True,
                           stdin = subprocess.PIPE,
                           stdout = subprocess.PIPE,
                           stderr = subprocess.PIPE,
                           universal_newlines = True)
    out, err = clt.communicate(input)
    sys.stdout.write(out)
    sys.stderr.write(err)

def main():
    xq_client = os.getenv('XQUERY_CLIENT')
    client('%s --input=my-document --collection=my-collection' % xq_client,
           '<document>test document</document>')
    client('%s -s "pf:documents()"' % xq_client)
    client('%s -s "pf:del-doc(\'my-document\')"' % xq_client)

main()
