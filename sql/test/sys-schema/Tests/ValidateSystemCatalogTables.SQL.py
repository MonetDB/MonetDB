import os, sys
from subprocess import run, PIPE, CalledProcessError

HOST=os.getenv('HOST')
MAPIPORT=os.getenv('MAPIPORT')
TSTDB=os.getenv('TSTDB')
TSTSRCBASE=os.getenv('TSTSRCBASE')
TSTDIR=os.getenv('TSTDIR')
CLIENT='org.monetdb.client.JdbcClient'
USER='monetdb'
PASSWORD='monetdb'


if __name__ == '__main__':
    with open(os.path.join('.monetdb'), 'w') as f:
        f.write('\n'.join(['user=monetdb', 'password=monetdb']))
    cmd = ['java', CLIENT, '-h', HOST, '-p', MAPIPORT, '-d', TSTDB, '-f', os.path.join(TSTSRCBASE, TSTDIR, 'Tests', 'ValidateSystemCatalogTables.sql')]
    try:
        p = run(cmd, stdout=PIPE, stderr=PIPE, check=True, encoding='utf-8')
        sys.stderr.write(p.stdout)
        sys.stderr.write(p.stderr)
    except CalledProcessError as e:
        raise SystemExit(e.stderr)
