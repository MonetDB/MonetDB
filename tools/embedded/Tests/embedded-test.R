library(MonetDB)
monetdb_embedded_startup("/tmp/mydb", T)

monetdb_embedded_query("CREATE TABLE FOO(i INTEGER, j STRING)")
monetdb_embedded_query("INSERT INTO FOO VALUES(42, 'Hello'), (84, 'World')")
monetdb_embedded_query("SELECT * FROM FOO")
monetdb_embedded_query("SELECT * FROM TABLES LIMIT 10")

monetdb_embedded_query("SELECT 42")
monetdb_embedded_query("SELECT 42 AS one, 43 AS two")
monetdb_embedded_query("SELECT 'Hello, World' AS val")

monetdb_embedded_query("SELECT COUNT(*) AS foundfoo FROM TABLES WHERE name='foo'")
monetdb_embedded_query("ROLLBACK")
monetdb_embedded_query("SELECT COUNT(*) AS foundfoo FROM TABLES WHERE name='foo'")

#install.packages("~/source/monetdb-embedded/tools/reverserapi/", repos=NULL)
