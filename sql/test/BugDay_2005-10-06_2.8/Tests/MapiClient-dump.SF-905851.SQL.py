import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

def client(cmd, infile = None):
    if infile is not None:
        f = open(infile)
    else:
        f = None
    clt = process.client(cmd, stdin = f,
                         stdout = process.PIPE, stderr = process.PIPE,
                         log = True)
    if f is not None:
        f.close()
    out, err = clt.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)

def main():
    client('sql',
           os.path.join(os.getenv('TSTSRCDIR'),
                        'JdbcClient_create_tables.sql'))
    client('sql',
           os.path.join(os.getenv('TSTSRCDIR'),
                        'JdbcClient_inserts_selects.sql'))
    client('sqldump')

main()
