import os, sys
from MonetDBtesting import process

host = os.getenv('HOST', 'localhost')
port = os.getenv('MAPIPORT', 50000)
db = os.getenv('TSTDB', 'demo')
mapihost = os.getenv('MAPIHOST', '/tmp') # not used on Windows

with process.client('sql',
                    dbname=f'mapi:monetdb://{host}:{port}/{db}?language=sql&user=monetdb',
                    host='',
                    port='',
                    format='csv',
                    echo=False,
                    stdin=process.PIPE,
                    stdout=process.PIPE,
                    stderr=process.PIPE) as c:
    out, err = c.communicate('select 1;\n')

if out != '1\n':
    print(out)
if err:
    print(err, file=sys.stderr)

with process.client('sql',
                    dbname=f'monetdb://{host}:{port}/{db}?language=sql&user=monetdb&password=monetdb',
                    host='',
                    port='',
                    format='csv',
                    echo=False,
                    stdin=process.PIPE,
                    stdout=process.PIPE,
                    stderr=process.PIPE) as c:
    out, err = c.communicate('select 1;\n')

if out != '1\n':
    print(out)
if err:
    print(err, file=sys.stderr)

if os.name != 'nt':
    with process.client('sql',
                        dbname=f'mapi:monetdb://{mapihost}/.s.monetdb.{port}?database={db}&language=sql&user=monetdb',
                        host='',
                        port='',
                        format='csv',
                        echo=False,
                        stdin=process.PIPE,
                        stdout=process.PIPE,
                        stderr=process.PIPE) as c:
        out, err = c.communicate('select 1;\n')

    if out != '1\n':
        print(out)
    if err:
        print(err, file=sys.stderr)

    with process.client('sql',
                        dbname=f'monetdb:///{db}?sock={mapihost}/.s.monetdb.{port}&language=sql&user=monetdb&password=monetdb',
                        host='',
                        port='',
                        format='csv',
                        echo=False,
                        stdin=process.PIPE,
                        stdout=process.PIPE,
                        stderr=process.PIPE) as c:
        out, err = c.communicate('select 1;\n')

    if out != '1\n':
        print(out)
    if err:
        print(err, file=sys.stderr)
