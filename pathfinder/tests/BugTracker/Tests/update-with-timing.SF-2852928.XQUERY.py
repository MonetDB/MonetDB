import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

def client(lang, args, input = None):
    clt = process.client(lang, args,
                         stdin = process.PIPE,
                         stdout = process.PIPE,
                         stderr = process.PIPE)
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
    # HACK ALERT: create updatable document by appending ,10 to collection name
    client('xquery',
           ['--input=hello-SF.2852928.xml',
            '--collection=hello-SF.2852928.xml,10'],
           hellodoc)
    sys.stderr.write('#~BeginVariableOutput~#\n')
    client('xquery',
           ['-t',
            '-s', 'do insert <a/> into doc("hello-SF.2852928.xml")/hello'])
    sys.stderr.write('#~EndVariableOutput~#\n')
    client('xquery', ['-s', 'doc("hello-SF.2852928.xml")'])
    client('xquery', ['-s', 'pf:del-doc("hello-SF.2852928.xml")'])

main()
