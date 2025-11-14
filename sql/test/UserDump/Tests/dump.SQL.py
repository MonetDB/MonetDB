from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    # dump with default user
    tc.sqldump().assertMatchStableOut(fout='dump.stable.out')
