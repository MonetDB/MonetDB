from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    tc.execute("select * from sys.malfunctions() order by module, function, address, signature, comment", client='mclient')\
            .assertSucceeded()
            .assertMatchStableOut("MAL-signitures.stable.out")
