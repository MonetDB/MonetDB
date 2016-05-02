
library(testthat)

basedir <- Sys.getenv("TSTTRGDIR")
if (basedir != "") {
	library(MonetDBLite, quietly=T, lib.loc=file.path(basedir, "rlibdir"))
} else {
	library(MonetDBLite)
}
dbdir <- file.path(tempdir(), "db1")
dbdir2 <- file.path(tempdir(), "db2")

test_that("db starts up", {
	expect_error(monetdb_embedded_startup("/dev/null"))
	dbdir <- tempdir()
	expect_equal(monetdb_embedded_startup(dbdir), TRUE)
	expect_error(monetdb_embedded_startup("/tmp"))
	expect_equal(monetdb_embedded_startup(dbdir), TRUE)
})


test_that("one can connect", {
	con <- monetdb_embedded_connect()
	expect_that(con, is_a("monetdb_embedded_connection"))
	monetdb_embedded_disconnect(con)
	# closed connections can be closed again
	monetdb_embedded_disconnect(con)
	expect_error(monetdb_embedded_disconnect(NULL))
	expect_error(monetdb_embedded_disconnect(42))
})

test_that("db runs queries and returns results", {
	con <- monetdb_embedded_connect()
	res <- monetdb_embedded_query(con, "SELECT 42")
	expect_equal(res$type, 1)
	expect_equal(res$tuples$single_value, 42)
	res <- monetdb_embedded_query(con, "SELECT * FROM tables")
	expect_equal(res$type, 1)
	expect_is(res$tuples, "data.frame")
	expect_true(nrow(res$tuples) > 0)
	expect_true(ncol(res$tuples) > 0)
	monetdb_embedded_disconnect(con)
})

test_that("a disconnected connection cannot be used", {
	con <- monetdb_embedded_connect()
	monetdb_embedded_query(con, "SELECT 42")
	monetdb_embedded_disconnect(con)
	res <- monetdb_embedded_query(con, "SELECT 42")
	expect_equal(res$type, "!")
})

test_that("commit", {
	con <- monetdb_embedded_connect()
	monetdb_embedded_query(con, "START TRANSACTION")
	monetdb_embedded_query(con, "CREATE TABLE foo (i integer)")
	monetdb_embedded_query(con, "INSERT INTO foo VALUES (42)")
	res <- monetdb_embedded_query(con, "SELECT i FROM foo")
	expect_equal(res$tuples$i, 42)
	monetdb_embedded_query(con, "COMMIT")
	res <- monetdb_embedded_query(con, "SELECT i FROM foo")
	expect_equal(res$tuples$i, 42)
	res <- monetdb_embedded_query(con, "SELECT * FROM tables WHERE name='foo'")
	expect_equal(nrow(res$tuples), 1)
	monetdb_embedded_query(con, "DROP TABLE foo")
	monetdb_embedded_disconnect(con)
})

test_that("rollback works", {
	con <- monetdb_embedded_connect()
	monetdb_embedded_query(con, "START TRANSACTION")
	monetdb_embedded_query(con, "CREATE TABLE foo (i integer)")
	monetdb_embedded_query(con, "INSERT INTO foo VALUES (42)")
	res <- monetdb_embedded_query(con, "SELECT i FROM foo")
	expect_equal(res$tuples$i, 42)
	monetdb_embedded_query(con, "ROLLBACK")
	res <- monetdb_embedded_query(con, "SELECT * FROM tables WHERE name='foo'")
	expect_equal(nrow(res$tuples), 0)
	res <- monetdb_embedded_query(con, "SELECT i FROM foo")
	expect_equal(res$type, "!")
	monetdb_embedded_disconnect(con)
})

test_that("rollback with errors", {
	con <- monetdb_embedded_connect()
	monetdb_embedded_query(con,"START TRANSACTION")
	res <- monetdb_embedded_query(con, "BULLSHIT")
	expect_equal(res$type, "!")
	res <- monetdb_embedded_query(con, "SELECT 1")
	expect_equal(res$type, "!")
	monetdb_embedded_query(con, "ROLLBACK")
	res <- monetdb_embedded_query(con, "SELECT 42")
	expect_equal(res$type, 1)
	monetdb_embedded_disconnect(con)
})

test_that("transaction immediately after rollback", {
	con <- monetdb_embedded_connect()
	monetdb_embedded_query(con, "START TRANSACTION")
	res <- monetdb_embedded_query(con, "SELECT 42")
	expect_equal(res$type, 1)
	monetdb_embedded_query(con, "ROLLBACK")
	monetdb_embedded_query(con, "START TRANSACTION")
	res <- monetdb_embedded_query(con, "SELECT 42")
	expect_equal(res$type, 1)
	monetdb_embedded_disconnect(con)
})

test_that("pointless rollback/commit", {
	con <- monetdb_embedded_connect()
	expect_equal(monetdb_embedded_query(con, "SELECT 1")$type, 1)
	monetdb_embedded_query(con, "COMMIT")
	expect_equal(monetdb_embedded_query(con, "SELECT 1")$type, 1)
	monetdb_embedded_query(con, "ROLLBACK")
	expect_equal(monetdb_embedded_query(con, "SELECT 1")$type, 1)
	monetdb_embedded_disconnect(con)
})

test_that("inserting data", {
	con <- monetdb_embedded_connect()
	monetdb_embedded_query(con, "CREATE TABLE foo(i INTEGER, j INTEGER)")
	monetdb_embedded_append(con, "foo", data.frame(i=1:10, j=21:30))
	res <- monetdb_embedded_query(con, "SELECT * FROM foo")
	expect_equal(nrow(res$tuples), 10)
	expect_equal(ncol(res$tuples), 2)
	expect_equal(res$tuples$i, 1:10)
	expect_equal(res$tuples$j, 21:30)
	monetdb_embedded_query(con, "DROP TABLE foo")
	monetdb_embedded_disconnect(con)
})

test_that("selecting null works", {
	con <- monetdb_embedded_connect()

	res <- monetdb_embedded_query(con, "SELECT NULL as nl")
	expect_equal(nrow(res$tuples), 1)
	expect_equal(ncol(res$tuples), 1)
	expect_true(is.na(res$tuples$nl))

	res <- monetdb_embedded_query(con, "SELECT NULL AS nl, name FROM tables")
	expect_true(nrow(res$tuples) > 1)
	expect_equal(ncol(res$tuples), 2)
	expect_true(all(is.na(res$tuples$nl)))

	monetdb_embedded_disconnect(con)
})

test_that("the garbage collector closes connections", {
	# there are 64 connections max. if gc() does not close them, the second batch will fail
	conns <- lapply(1:64, function(x) monetdb_embedded_connect())
	expect_error(monetdb_embedded_connect())

	rm(conns)
	gc()

	conns <- lapply(1:64, function(x) monetdb_embedded_connect())
	rm(conns)
	gc()
})


test_that("the logger does not misbehave", {
	con <- monetdb_embedded_connect()
	monetdb_embedded_query(con, "CREATE TABLE foo(i INTEGER, j INTEGER)")
	Sys.sleep(5)
	monetdb_embedded_query(con, "DROP TABLE foo")
	monetdb_embedded_disconnect(con)
})


test_that("shutdown does work", {
	monetdb_embedded_shutdown()
	monetdb_embedded_shutdown()
})


test_that("starting up in same dir again works", {
	monetdb_embedded_startup(dbdir)
	con <- monetdb_embedded_connect()
	monetdb_embedded_query(con, "CREATE TABLE foo1(a1 INTEGER)")
	monetdb_embedded_query(con, "INSERT INTO foo1 VALUES(42)")
	res <- monetdb_embedded_query(con, "SELECT a1 FROM foo1")
	expect_equal(nrow(res$tuples), 1)
	expect_equal(ncol(res$tuples), 1)
	expect_equal(res$tuples$a1, 42)

	monetdb_embedded_disconnect(con)
	monetdb_embedded_shutdown()
})



test_that("connections from shut down db's dont work", {
	monetdb_embedded_startup(dbdir)
	con <- monetdb_embedded_connect()
	monetdb_embedded_shutdown()
	expect_equal(monetdb_embedded_query(con, "SELECT * FROM tables")$type, "!")
	monetdb_embedded_disconnect(con)
})


test_that("db starts up in other directory", {
	monetdb_embedded_startup(dbdir2)
	con <- monetdb_embedded_connect()
	expect_equal(monetdb_embedded_query(con, "SELECT * FROM foo1")$type, "!")
	monetdb_embedded_query(con, "CREATE TABLE foo2(a2 INTEGER)")
	monetdb_embedded_query(con, "INSERT INTO foo2 VALUES(84)")
	res <- monetdb_embedded_query(con, "SELECT a2 FROM foo2")
	expect_equal(nrow(res$tuples), 1)
	expect_equal(ncol(res$tuples), 1)
	expect_equal(res$tuples$a2, 84)

	monetdb_embedded_disconnect(con)
	monetdb_embedded_shutdown()
})

test_that("connections from previous run cannot be reused", {
	monetdb_embedded_startup(dbdir)
	con <- monetdb_embedded_connect()
	monetdb_embedded_shutdown()
	monetdb_embedded_startup(dbdir2)
	expect_equal(monetdb_embedded_query(con, "SELECT * FROM tables")$type, "!")
	monetdb_embedded_disconnect(con)
	monetdb_embedded_shutdown()
})



