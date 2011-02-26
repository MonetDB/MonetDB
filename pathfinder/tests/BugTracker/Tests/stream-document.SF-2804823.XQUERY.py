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

def main():
    client('xquery', ['--input=my-document', '--collection=my-collection'],
           '<document>test document</document>')
    client('xquery',
           ['-s', 'for $doc in pf:documents() where $doc/@url = "my-document" return $doc'])
    client('xquery', ['-s', 'pf:del-doc("my-document")'])

main()
