from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    # dump with default user
    tc.connect(username='monetdb', password='monetdb')
    tc.sqldump().assertMatchStableOut(fout='dump.stable.out')
