
from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    tc.connect()
    tc.execute("trace select count(*) FROM tables").assertSucceeded()
