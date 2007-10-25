import os, sys
try:
    import subprocess
except ImportError:
    # use private copy for old Python versions
    import MonetDB.subprocess26 as subprocess


def client(cmd, infile = None):
    if infile is not None:
        f = open(infile)
    else:
        f = None
    clt = subprocess.Popen(cmd, shell=True, stdin = f)
    if f is not None:
        f.close()
    clt.wait()


def main():
    client('Mlog -x %s' % os.getenv('SQL_CLIENT'),
           os.path.join(os.getenv('RELSRCDIR'),
                        'JdbcClient_create_tables.sql'))
    client('Mlog -x %s' % os.getenv('SQL_CLIENT'),
           os.path.join(os.getenv('RELSRCDIR'),
                        'JdbcClient_inserts_selects.sql'))
    client('Mlog -x %s' % os.getenv('SQL_DUMP'))

main()
