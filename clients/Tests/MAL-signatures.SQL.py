import os
from MonetDBtesting.sqltest import SQLTestCase

qry="""select * from sys.malfunctions() order by module, "function", address, signature, comment;"""
fout = 'MAL-signatures.stable.out.int128' if os.environ.get('HAVE_HGE') else 'MAL-signatures.stable.out'
with SQLTestCase() as tc:
    tc.execute(qry, client='mclient')\
            .assertSucceeded()\
            .assertMatchStableOut(fout)
