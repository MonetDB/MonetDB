from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    # dump with default user
    tc.connect(username='test', password='test')
    tc.sqldump().assertMatchStableOut(fout='dump_with_test.stable.out')
