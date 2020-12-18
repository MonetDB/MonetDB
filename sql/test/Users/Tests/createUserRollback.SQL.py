###
# Check that if a CREATE USER transaction is aborted, the user is not created
#   and hence cannot log in
###

from MonetDBtesting.sqltest import SQLTestCase
import logging

logging.basicConfig(level=logging.FATAL)

with SQLTestCase() as tc:
    tc.connect(username="monetdb", password="monetdb")
    tc.execute("CREATE TABLE sys.myvar (c BIGINT);")
    tc.execute("INSERT INTO sys.myvar VALUES ((SELECT COUNT(*) FROM sys.users));")
    tc.execute("""
                START TRANSACTION;
                CREATE USER "1" WITH PASSWORD '1' NAME '1' SCHEMA "sys";
                ROLLBACK;

                SELECT CAST(COUNT(*) - (SELECT c FROM sys.myvar) AS BIGINT) FROM sys.users; -- The MAL authorization is not transaction aware, so the count changes :/
                DROP TABLE sys.myvar;
            """).assertSucceeded()
    tc.connect(username="1", password="1")
    tc.execute("SELECT 1;")

