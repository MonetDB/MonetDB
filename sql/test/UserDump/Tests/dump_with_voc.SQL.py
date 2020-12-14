from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    # dump with default user
    tc.connect(username='voc', password='voc')
    tc.sqldump().assertMatchStableOut(fout='dump_with_voc.stable.out')
