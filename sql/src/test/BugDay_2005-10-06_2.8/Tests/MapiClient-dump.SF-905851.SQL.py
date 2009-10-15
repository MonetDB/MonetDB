import os, sys, time
from MonetDBtesting import process


def client(cmd, infile = None):
    if infile is not None:
        f = open(infile)
    else:
        f = None
    Mlog = "\n%s  %s\n\n" % (time.strftime('# %H:%M:%S >',time.localtime(time.time())), cmd)
    sys.stdout.write(Mlog)
    sys.stderr.write(Mlog)
    clt = process.client(cmd, stdin = f)
    if f is not None:
        f.close()
    clt.wait()


def main():
    client('sql',
           os.path.join(os.getenv('TSTSRCDIR'),
                        'JdbcClient_create_tables.sql'))
    client('sql',
           os.path.join(os.getenv('TSTSRCDIR'),
                        'JdbcClient_inserts_selects.sql'))
    client('sqldump')

main()
