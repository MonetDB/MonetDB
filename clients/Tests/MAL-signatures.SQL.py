from MonetDBtesting.sqltest import SQLTestCase
qry="""select * from sys.malfunctions() order by module, "function", address, signature, comment;"""
with SQLTestCase() as tc:
    tc.execute(qry, client='mclient')\
            .assertSucceeded()\
            .assertMatchStableOut("MAL-signatures.stable.out")
