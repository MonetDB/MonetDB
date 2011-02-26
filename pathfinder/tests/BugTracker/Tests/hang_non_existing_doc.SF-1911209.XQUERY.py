import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

def client(lang, args, input = None):
    clt = process.client(lang, args = args,
                         stdin = process.PIPE,
                         stdout = process.PIPE,
                         stderr = process.PIPE)
    out, err = clt.communicate(input)
    sys.stdout.write(out)
    sys.stderr.write(err)

def main():
    # HACK ALERT: create updatable document by appending ,10 to collection name
    client('xquery',
           ['--input=doc1911209.xml', '--collection=doc1911209.xml,10'],
           '<aap/>')
    client('xquery', ['-s', 'do insert <beer/> into doc("doc1911209.xml")/aap'])
    for i in range(1000):
        client('xquery', ['-s', 'pf:documents()'])
        client('xquery', ['-s', 'doc("does_not_exist.xml")'])
    client('xquery', ['-s', 'pf:del-doc("doc1911209.xml")'])

main()
