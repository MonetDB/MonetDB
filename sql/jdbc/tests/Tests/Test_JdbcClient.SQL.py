import os, sys, glob
from subprocess import run, PIPE, CalledProcessError

HOST=os.getenv('HOST')
MAPIPORT=os.getenv('MAPIPORT')
TSTDB=os.getenv('TSTDB')
TSTSRCBASE=os.getenv('TSTSRCBASE')
TSTTRGBASE=os.getenv('TSTTRGBASE')
TSTDIR=os.getenv('TSTDIR')
CLIENT='org.monetdb.client.JdbcClient'
USER='monetdb'
PASSWORD='monetdb'

if __name__ == '__main__':
    with open(os.path.join('.monetdb'), 'w') as f:
        f.write('\n'.join(['user=monetdb', 'password=monetdb']))

    cmd = ['java', CLIENT, '-h', HOST, '-p', MAPIPORT, '-d', TSTDB, '--help']
    try:
        p = run(cmd, stdout=PIPE, stderr=PIPE, check=True, encoding='utf-8')
    except CalledProcessError as e:
        raise SystemExit(e.stderr)

    cmd = ['java', CLIENT, '-h', HOST, '-p', MAPIPORT, '-d', TSTDB, '-f', os.path.join(TSTSRCBASE, TSTDIR, 'Tests', 'JdbcClient_create_tables.sql')]
    try:
        p = run(cmd, stdout=PIPE, stderr=PIPE, check=True, encoding='utf-8')
    except CalledProcessError as e:
        print(e.stderr, file=sys.stderr)
        raise SystemExit('ERROR: failed to create tables!')

    cmd = ['java', CLIENT, '-h', HOST, '-p', MAPIPORT, '-d', TSTDB, '-f', os.path.join(TSTSRCBASE, TSTDIR, 'Tests', 'JdbcClient_inserts_selects.sql')]
    try:
        p = run(cmd, stdout=PIPE, stderr=PIPE, check=True, encoding='utf-8')
    except CalledProcessError as e:
        print(e.stderr, file=sys.stderr)
        raise SystemExit('ERROR: failed to insert!')

    cmd = ['java', CLIENT, '-h', HOST, '-p', MAPIPORT, '-d', TSTDB, '-D']
    try:
        p = run(cmd, stdout=PIPE, stderr=PIPE, check=True, encoding='utf-8')
    except CalledProcessError as e:
        raise SystemExit(e.stderr)

    cmd = ['java', CLIENT, '-h', HOST, '-p', MAPIPORT, '-d', TSTDB, '-f', os.path.join(TSTSRCBASE, TSTDIR, 'Tests', 'JdbcClient_drop_tables.sql')]
    try:
        p = run(cmd, stdout=PIPE, stderr=PIPE, check=True, encoding='utf-8')
    except CalledProcessError as e:
        raise SystemExit(e.stderr)

    try:
        # test the ON CLIENT download/export functionality via JdbcClient with --csvdir argument (to enable the ON CLIENT functionality)
        cmd = ['java', CLIENT, '-h', HOST, '-p', MAPIPORT, '-d', TSTDB, '--csvdir', os.path.join(TSTSRCBASE, TSTDIR, 'Tests'), '-f', os.path.join(TSTSRCBASE, TSTDIR, 'Tests', 'OnClientDownloadData.sql')]
        p = run(cmd, stdout=PIPE, stderr=PIPE, check=True, encoding='utf-8')
        # test the ON CLIENT upload/import functionality via JdbcClient with --csvdir argument (to enable the ON CLIENT functionality)
        cmd = ['java', CLIENT, '-h', HOST, '-p', MAPIPORT, '-d', TSTDB, '--csvdir', os.path.join(TSTSRCBASE, TSTDIR, 'Tests'), '-f', os.path.join(TSTSRCBASE, TSTDIR, 'Tests', 'OnClientUploadData.sql')]
        p = run(cmd, stdout=PIPE, stderr=PIPE, check=True, encoding='utf-8')
    except CalledProcessError as e:
        sys.stderr.write(str(e))
    finally:
        # cleanup created data export files from Tests/DownloadData.sql
        for tfile in glob.glob(os.path.join(TSTSRCBASE, TSTDIR, 'Tests', 'sys_tables_by_id.*')):
            os.remove(tfile)

#set -e
#
#cat << EOF > .monetdb
#user=monetdb
#password=monetdb
#EOF
#
#java org.monetdb.client.JdbcClient -h $HOST -p $MAPIPORT -d $TSTDB --help
#
#java org.monetdb.client.JdbcClient -h $HOST -p $MAPIPORT -d $TSTDB -f $TSTSRCBASE/$TSTDIR/Tests/JdbcClient_create_tables.sql
#java org.monetdb.client.JdbcClient -h $HOST -p $MAPIPORT -d $TSTDB -f $TSTSRCBASE/$TSTDIR/Tests/JdbcClient_inserts_selects.sql
#java org.monetdb.client.JdbcClient -h $HOST -p $MAPIPORT -d $TSTDB -D
#java org.monetdb.client.JdbcClient -h $HOST -p $MAPIPORT -d $TSTDB -f $TSTSRCBASE/$TSTDIR/Tests/JdbcClient_drop_tables.sql
#
#rm -f .monetdb
