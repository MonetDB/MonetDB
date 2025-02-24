from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    tc.connect(username="monetdb", password="monetdb")
    tc.execute("CREATE TABLE authors(lastname VARCHAR(20), firstname VARCHAR(20), PRIMARY KEY (lastname, firstname));")
    tc.execute("CREATE TABLE books_partial (authlast VARCHAR(20), authfirst VARCHAR(20), FOREIGN KEY (authlast, authfirst) REFERENCES authors(lastname, firstname) MATCH PARTIAL);")
    tc.execute("CREATE TABLE books_full (authlast VARCHAR(20), authfirst VARCHAR(20), FOREIGN KEY (authlast, authfirst) REFERENCES authors(lastname, firstname) MATCH FULL);")
    d = tc.sqldump()
    d.assertMatchStableOut(fout='7598-foreign_key_match_options_dump.stable.out', ratio=1)
    tc.execute("DROP TABLE books_full;")
    tc.execute("DROP TABLE books_partial;")
    tc.execute("DROP TABLE authors;")
