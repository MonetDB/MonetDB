from MonetDBtesting.sqltest import SQLTestCase
import os

with SQLTestCase() as tc:
    tc.sqldump().assertMatchStableOut(fout='mclient-lsql-D.stable.out')
