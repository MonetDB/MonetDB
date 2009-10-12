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

hellodoc = r'''
<hello x="y" lang="en">
  <foo>text of node foo<bar>text of node bar</bar></foo>
  <!-- a comment node -->
  <?pi action=beep?>
  <world/>
  <path>\asfda\asdfasf\asdf</path>
</hello>
'''

def main():
    xq_client = os.getenv('XQUERY_CLIENT').split()
    # HACK ALERT: create updatable document by appending ,10 to collection name
    client(xq_client + ['--input=hello-SF.2852928.xml', '--collection=hello-SF.2852928.xml,10'],
           hellodoc)
    sys.stdout.write('#~BeginVariableOutput~#\n')
    client(xq_client + ['-t', '-s', 'do insert <a/> into doc("hello-SF.2852928.xml")/hello'])
    sys.stdout.write('#~EndVariableOutput~#\n')
    client(xq_client + ['-s', 'doc("hello-SF.2852928.xml")'])
    client(xq_client + ['-s', 'pf:del-doc("hello-SF.2852928.xml")'])

main()
