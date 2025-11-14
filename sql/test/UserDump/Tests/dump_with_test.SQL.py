from MonetDBtesting.sqltest import SQLTestCase

# dump with test user
with SQLTestCase(username='test', password='test') as tc:
    tc.sqldump().assertMatchStableOut(fout='dump_with_test.stable.out')
