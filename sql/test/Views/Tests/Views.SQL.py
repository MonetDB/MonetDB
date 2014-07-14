import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

def client(lang, infile):
    clt = process.client(lang, stdin=open(infile), stdout=process.PIPE, stderr=process.PIPE)
    out, err = clt.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)

def main():
    sys.stdout.write('Views Restrictions\n')
    client('sql', os.path.join(os.getenv('RELSRCDIR'), os.pardir,
                               'views_restrictions.sql'))
    sys.stdout.write('step 1\n')
    sys.stdout.write('Cleanup\n')
    sys.stdout.write('step2\n')

main()
