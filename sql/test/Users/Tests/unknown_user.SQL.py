###
# Authenticate unknown USER (not possible).
###

import sys,os
import pymonetdb

import logging

logging.basicConfig(level=logging.FATAL)

error = False
db=os.getenv("TSTDB")
port=int(os.getenv("MAPIPORT"))
try:
    client = pymonetdb.connect(database=db, port=port, autocommit=True,
        user='this_user_does_not_exist', password='this_password_does_not_exist')
except pymonetdb.exceptions.DatabaseError:
    error = True
    pass

if not error:
    print("Logged in with invalid credentials")
    sys.exit(-1)
