library(testthat)
library(MonetDBLite)

test_that("db starts up", {
	expect_error(monetdb_embedded_startup("/dev/null"))
	expect_equal(monetdb_embedded_startup(), TRUE)
	expect_warning(monetdb_embedded_startup())
})

test_that("db runs queries and returns results", {
	res <- monetdb_embedded_query("SELECT 42")
	expect_equal(res$type, 1)
	expect_equal(res$tuples$single_value, 42)
	res <- monetdb_embedded_query("SELECT * FROM tables")
	expect_equal(res$type, 1)
	expect_is(res$tuples, "data.frame")
	expect_true(nrow(res$tuples) > 0)
	expect_true(ncol(res$tuples) > 0)
})

test_that("commit", {
	monetdb_embedded_query("START TRANSACTION")
	monetdb_embedded_query("CREATE TABLE foo (i integer)")
	monetdb_embedded_query("INSERT INTO foo VALUES (42)")
	res <- monetdb_embedded_query("SELECT i FROM foo")
	expect_equal(res$tuples$i, 42)
	monetdb_embedded_query("COMMIT")
	res <- monetdb_embedded_query("SELECT i FROM foo")
	expect_equal(res$tuples$i, 42)
	res <- monetdb_embedded_query("SELECT * FROM tables WHERE name='foo'")
	expect_equal(nrow(res$tuples), 1)
	monetdb_embedded_query("DROP TABLE foo")
})

test_that("rollback works", {
	monetdb_embedded_query("START TRANSACTION")
	monetdb_embedded_query("CREATE TABLE foo (i integer)")
	monetdb_embedded_query("INSERT INTO foo VALUES (42)")
	res <- monetdb_embedded_query("SELECT i FROM foo")
	expect_equal(res$tuples$i, 42)
	monetdb_embedded_query("ROLLBACK")
	res <- monetdb_embedded_query("SELECT * FROM tables WHERE name='foo'")
	expect_equal(nrow(res$tuples), 0)
	res <- monetdb_embedded_query("SELECT i FROM foo")
	expect_equal(res$type, "!")
})

test_that("rollback with errors", {
	monetdb_embedded_query("START TRANSACTION")
	res <- monetdb_embedded_query("BULLSHIT")
	expect_equal(res$type, "!")
	res <- monetdb_embedded_query("SELECT 1")
	expect_equal(res$type, "!")
	monetdb_embedded_query("ROLLBACK")
	res <- monetdb_embedded_query("SELECT 42")
	expect_equal(res$type, 1)
})

test_that("pointless rollback/commit", {
	expect_equal(monetdb_embedded_query("SELECT 1")$type, 1)
	monetdb_embedded_query("COMMIT")
	expect_equal(monetdb_embedded_query("SELECT 1")$type, 1)
	monetdb_embedded_query("ROLLBACK")
	expect_equal(monetdb_embedded_query("SELECT 1")$type, 1)
})

test_that("inserting data", {
	monetdb_embedded_query("CREATE TABLE foo(i INTEGER, j INTEGER)")
	monetdb_embedded_append("foo", data.frame(i=1:10, j=21:30))
	res <- monetdb_embedded_query("SELECT * FROM foo")
	expect_equal(nrow(res$tuples), 10)
	expect_equal(ncol(res$tuples), 2)
	expect_equal(res$tuples$i, 1:10)
	expect_equal(res$tuples$j, 21:30)
	monetdb_embedded_query("DROP TABLE foo")
})

test_that("the logger does not misbehave", {
	monetdb_embedded_query("CREATE TABLE foo(i INTEGER, j INTEGER)")
	Sys.sleep(5)
	monetdb_embedded_query("DROP TABLE foo")
})

# test_that("bigger data", {
# 	monetdb_embedded_query("CREATE TABLE lineitem (l_orderkey INT NOT NULL, l_partkey INT NOT NULL, l_suppkey INT NOT NULL, l_linenumber INT NOT NULL, l_quantity INTEGER NOT NULL, l_extendedprice DECIMAL(15,2) NOT NULL, l_discount DECIMAL(15,2) NOT NULL, l_tax DECIMAL(15,2) NOT NULL, l_returnflag VARCHAR(1) NOT NULL, l_linestatus VARCHAR(1) NOT NULL, l_shipdate DATE NOT NULL, l_commitdate DATE NOT NULL, l_receiptdate DATE NOT NULL, l_shipinstruct VARCHAR(25) NOT NULL, l_shipmode VARCHAR(10) NOT NULL, l_comment VARCHAR(44) NOT NULL, PRIMARY KEY (l_orderkey,l_linenumber))")
# 	monetdb_embedded_query("COPY INTO lineitem FROM '/Users/hannes/Desktop/lineitem.gz'")
# 	res <- monetdb_embedded_query("SELECT * FROM lineitem")
# 	expect_equal(res$type, 1)
# 	expect_equal(nrow(res$tuples), 1000000)
# 	expect_equal(ncol(res$tuples), 16)
# 	monetdb_embedded_query("DROP TABLE lineitem")
# })

