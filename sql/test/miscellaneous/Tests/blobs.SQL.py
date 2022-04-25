from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    tc.execute('''
    START TRANSACTION;
    CREATE TABLE myt(c0 BINARY LARGE OBJECT,c1 INTEGER);
    INSERT INTO myt VALUES
    (BINARY LARGE OBJECT '', 1),(BINARY LARGE OBJECT '', 1),
    (BINARY LARGE OBJECT 'AB', 1),(BINARY LARGE OBJECT 'A1EFD3', 1),
    (NULL, NULL),(NULL, 1),(BINARY LARGE OBJECT '', NULL);
    COMMIT;''').assertSucceeded()
    tc.sqldump().assertMatchStableOut(fout='blobs.stable.out')
    tc.execute('drop table myt;').assertSucceeded()
