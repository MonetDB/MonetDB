###
# Check that to execute COPY INTO and COPY FROM, a user does not only need the
#   INSERT and SELECT privileges but also the COPY INTO and COPY FROM
#   privileges.
###

import os, sys
from MonetDBtesting.sqltest import SQLTestCase

BASEDIR = os.environ.get('TSTTRGDIR')
INPUT = os.path.join(BASEDIR, "copyfrom.csv")
OUTPUT = os.path.join(BASEDIR, "copyinto.csv")

with open(INPUT, 'w') as csvin:
    csvin.write("24\n42\n")

with SQLTestCase() as mdb:
    mdb.connect(username="monetdb", password="monetdb")
    mdb.execute("CREATE TABLE t (i INT);").assertSucceeded()
    mdb.execute("COPY 2 RECORDS INTO t FROM r'%s' USING DELIMITERS ',', E'\\n','\"';" % INPUT).assertSucceeded()
    mdb.execute("CREATE USER copyuser WITH PASSWORD 'copyuser' name 'copyuser' schema sys;").assertSucceeded()

    # TODO: add check for COPY <INTO|FROM> STDIN
    with SQLTestCase() as usr:
        # The user has no privileges
        usr.connect(username="copyuser", password="copyuser")
        usr.execute("SELECT * FROM t;").assertFailed(err_code="42000", err_message="SELECT: access denied for copyuser to table 'sys.t'")
        usr.execute("INSERT INTO t VALUES (888);").assertFailed(err_code="42000", err_message="INSERT INTO: insufficient privileges for user 'copyuser' to insert into table 't'")
        usr.execute("COPY 2 RECORDS INTO t FROM r'%s' USING DELIMITERS ',', E'\\n','\"';" % INPUT).assertFailed(err_code="42000", err_message="COPY INTO: insufficient privileges for user 'copyuser' to copy into table 't'")

        # Check that for COPY INTO <table> FROM <file>, the user needs both
        #  INSERT and COPY FROM privileges
        mdb.execute("GRANT INSERT ON t TO copyuser;").assertSucceeded()
        usr.execute("INSERT INTO t VALUES (888);").assertSucceeded()
        usr.execute("COPY 2 RECORDS INTO t FROM r'%s' USING DELIMITERS ',', E'\\n','\"';" % INPUT).assertFailed(err_code="42000", err_message="COPY INTO: insufficient privileges: COPY INTO from file(s) requires database administrator rights, use 'COPY INTO \"t\" FROM file ON CLIENT' instead")
        mdb.execute("GRANT COPY FROM TO copyuser;").assertSucceeded()
        usr.execute("COPY 2 RECORDS INTO t FROM r'%s' USING DELIMITERS ',', E'\\n','\"';" % INPUT).assertSucceeded()

        mdb.execute("REVOKE INSERT ON t FROM copyuser;\n").assertSucceeded()
        usr.execute("COPY 2 RECORDS INTO t FROM r'%s' USING DELIMITERS ',', E'\\n','\"';" % INPUT).assertFailed(err_code="42000", err_message="COPY INTO: insufficient privileges for user 'copyuser' to copy into table 't'")
        mdb.execute("GRANT INSERT ON t TO copyuser;").assertSucceeded()
        mdb.execute("REVOKE COPY FROM FROM copyuser;\n").assertSucceeded()
        usr.execute("COPY 2 RECORDS INTO t FROM r'%s' USING DELIMITERS ',', E'\\n','\"';" % INPUT).assertFailed(err_code="42000", err_message="COPY INTO: insufficient privileges: COPY INTO from file(s) requires database administrator rights, use 'COPY INTO \"t\" FROM file ON CLIENT' instead")

        # Check that for COPY <table> INTO <file>, the user needs both SELECT
        #   and COPY INTO privileges
        mdb.execute("GRANT SELECT ON t TO copyuser;").assertSucceeded()
        usr.execute("SELECT * FROM t;").assertSucceeded().assertDataResultMatch([(24,), (42,), (888,), (24,), (42,)])
        usr.execute("COPY SELECT * FROM t INTO r'%s';" % OUTPUT).assertFailed(err_code="42000", err_message="COPY INTO: insufficient privileges: COPY INTO file requires database administrator rights, use 'COPY ... INTO file ON CLIENT' instead")

        mdb.execute("GRANT COPY INTO TO copyuser;").assertSucceeded()
        usr.execute("COPY SELECT * FROM t INTO r'%s';" % OUTPUT).assertSucceeded()

        mdb.execute("REVOKE COPY INTO FROM copyuser;\n").assertSucceeded()
        usr.execute("COPY SELECT * FROM t INTO r'%s';" % OUTPUT).assertFailed(err_code="42000", err_message="COPY INTO: insufficient privileges: COPY INTO file requires database administrator rights, use 'COPY ... INTO file ON CLIENT' instead")

        mdb.execute("GRANT COPY INTO TO copyuser;").assertSucceeded()
        mdb.execute("REVOKE SELECT ON t FROM copyuser;\n").assertSucceeded()
        usr.execute("COPY SELECT * FROM t INTO r'%s';" % OUTPUT).assertFailed(err_code="42000", err_message="SELECT: access denied for copyuser to table 'sys.t'")

