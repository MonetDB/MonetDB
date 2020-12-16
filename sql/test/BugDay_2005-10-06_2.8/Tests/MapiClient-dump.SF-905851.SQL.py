import os, sys, difflib
try:
    from MonetDBtesting import process
except ImportError:
    import process

def client(cmd, infile = None):
    if infile is not None:
        f = open(infile)
    else:
        f = None
    with process.client(cmd, stdin = f,
                        stdout = process.PIPE, stderr = process.PIPE) as clt:
        if f is not None:
            f.close()
        out, err = clt.communicate()
        sys.stderr.write(err)
        return out

def main():
    client('sql',
           os.path.join(os.getenv('TSTSRCDIR'),
                        'JdbcClient_create_tables.sql'))
    client('sql',
           os.path.join(os.getenv('TSTSRCDIR'),
                        'JdbcClient_inserts_selects.sql'))
    out = client('sqldump')
    output = out.splitlines(keepends=True)
    stable = open('MapiClient-dump.SF-905851.stable.out').readlines()
    for line in difflib.unified_diff(stable, output):
        sys.stderr.write(line)

main()
