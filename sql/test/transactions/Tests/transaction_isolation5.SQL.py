from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as mdb1:
    mdb1.connect(username="monetdb", password="monetdb")
    mdb1.execute('START TRANSACTION;').assertSucceeded()
    mdb1.execute('SAVEPOINT mys;').assertSucceeded()
    mdb1.execute('CREATE SCHEMA ups;').assertSucceeded()
    mdb1.execute('SET SCHEMA ups;').assertSucceeded()
    mdb1.execute('ROLLBACK TO SAVEPOINT mys;').assertFailed(err_code="40000", err_message="ROLLBACK: finished sucessfuly, but the session's schema could not be found on the current transaction")
    mdb1.execute('rollback;').assertFailed()

with SQLTestCase() as mdb1:
    mdb1.connect(username="monetdb", password="monetdb")
    mdb1.execute('START TRANSACTION;').assertSucceeded()
    mdb1.execute('SAVEPOINT mys2;').assertSucceeded()
    mdb1.execute('CREATE SCHEMA ups2;').assertSucceeded()
    mdb1.execute('SET SCHEMA ups2;').assertSucceeded()
    mdb1.execute('RELEASE SAVEPOINT mys2;').assertSucceeded()
    mdb1.execute('rollback;').assertFailed()
