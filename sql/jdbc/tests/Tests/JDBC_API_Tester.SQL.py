import os, sys
from subprocess import run, PIPE, CalledProcessError

HOST=os.getenv('HOST')
MAPIPORT=os.getenv('MAPIPORT')
TSTDB=os.getenv('TSTDB')
TSTSRCBASE=os.getenv('TSTSRCBASE')
TSTDIR=os.getenv('TSTDIR')
USER='monetdb'
PASSWORD='monetdb'
JDBC_EXTRA_ARGS=os.getenv('JDBC_EXTRA_ARGS', default='')

URL=f"jdbc:monetdb://{HOST}:{MAPIPORT}/{TSTDB}?user={USER}&password={PASSWORD}{JDBC_EXTRA_ARGS}"

JDBC_API_TESTER_ARGS=''
# JDBC_API_TESTER_ARGS='-skipMALoutput'

cmd = ['java', 'JDBC_API_Tester', URL, JDBC_API_TESTER_ARGS]
try:
    p = run(cmd, stdout=PIPE, stderr=PIPE, check=True, encoding='utf-8')
    sys.stderr.write(p.stdout)
    sys.stderr.write(p.stderr)
except CalledProcessError as e:
    raise SystemExit(e.stderr)



##!/bin/sh
#
#URL="jdbc:monetdb://${HOST}:${MAPIPORT}/${TSTDB}?user=monetdb&password=monetdb${JDBC_EXTRA_ARGS}"
#TST=$1
#
#java ${TST} "${URL}"
