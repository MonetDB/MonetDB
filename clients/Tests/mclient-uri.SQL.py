import os, sys
from MonetDBtesting import process

with process.client('sql', dbname='mapi:monetdb://{}:{}/{}?language=sql&user=monetdb'.format(os.getenv('HOST', 'localhost'), os.getenv('MAPIPORT', 50000), os.getenv('TSTDB', 'demo')), host='', port='', format='csv', echo=False, stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as c:
    out, err = c.communicate('select 1;\n')

if out != '1\n':
    print(out)
if err:
    print(err, file=sys.stderr)

if os.name != 'nt':
    with process.client('sql', dbname='mapi:monetdb://{}/.s.monetdb.{}?database={}&language=sql&user=monetdb'.format(os.getenv('MAPIHOST', '/tmp'), os.getenv('MAPIPORT', 50000), os.getenv('TSTDB', 'demo')), host='', port='', format='csv', echo=False, stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as c:
        out, err = c.communicate('select 1;\n')

    if out != '1\n':
        print(out)
    if err:
        print(err, file=sys.stderr)
