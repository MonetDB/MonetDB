from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    tc.sqldump('--inserts').assertMatchStableOut(fout='dump-empty.stable.out')


