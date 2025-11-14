from MonetDBtesting.sqltest import SQLTestCase

# dump with voc user
with SQLTestCase(username='voc', password='voc') as tc:
    tc.sqldump().assertMatchStableOut(fout='dump_with_voc.stable.out')
