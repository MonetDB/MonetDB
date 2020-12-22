import sys, os, pymonetdb
import logging

logging.basicConfig(level=logging.FATAL)

db = os.getenv("TSTDB")
port = int(os.getenv("MAPIPORT"))

try:
    pymonetdb.connect(database=db, port=port, autocommit=True, username='invalid', password='invalid')
    sys.stderr.write('Exception expected')
except Exception as ex:
    if 'invalid credentials for user \'invalid\'' not in str(ex):
        sys.stderr.write('Expected error: invalid credentials for user \'invalid\'')
