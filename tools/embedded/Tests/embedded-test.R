library(MonetDB)
monetdb_embedded_startup("/tmp/mydb", T)

monetdb_embedded_query("SELECT 42")
monetdb_embedded_query("SELECT 42;;")
monetdb_embedded_query("SELECT 42 AS one, 43 AS two")
monetdb_embedded_query("SELECT 'Hello, World' AS val")

monetdb_embedded_query("SELECT * FROM TABLES LIMIT 10")

monetdb_embedded_query("CREATE TABLE FOO(i INTEGER, j STRING)", commit=F)
monetdb_embedded_query("INSERT INTO FOO VALUES(42, 'Hello'), (84, 'World')", commit=F)
monetdb_embedded_query("SELECT * FROM FOO", commit=F)
monetdb_embedded_query("SELECT COUNT(*) AS foundfoo FROM TABLES WHERE name='foo'", commit=F)
monetdb_embedded_query("ROLLBACK", commit=F)

monetdb_embedded_query("SELECT COUNT(*) AS foundfoo FROM TABLES WHERE name='foo'")

#monetdb_embedded_query("thisiscertainlynotsql;")
# TODO: recover after parser errors! rollback?

monetdb_embedded_query("DROP TABLE lineitem");
monetdb_embedded_query("CREATE TABLE lineitem (l_orderkey INT NOT NULL, l_partkey INT NOT NULL, l_suppkey INT NOT NULL, l_linenumber INT NOT NULL, l_quantity INTEGER NOT NULL, l_extendedprice DECIMAL(15,2) NOT NULL, l_discount DECIMAL(15,2) NOT NULL, l_tax DECIMAL(15,2) NOT NULL, l_returnflag VARCHAR(1) NOT NULL, l_linestatus VARCHAR(1) NOT NULL, l_shipdate DATE NOT NULL, l_commitdate DATE NOT NULL, l_receiptdate DATE NOT NULL, l_shipinstruct VARCHAR(25) NOT NULL, l_shipmode VARCHAR(10) NOT NULL, l_comment VARCHAR(44) NOT NULL);")

monetdb_embedded_query("COPY INTO lineitem FROM '/tmp/lineitem6.tbl'")

monetdb_embedded_query("SELECT * FROM lineitem LIMIT 10;")
monetdb_embedded_query("SELECT COUNT(*) FROM lineitem")
monetdb_embedded_query("DROP TABLE lineitem");
