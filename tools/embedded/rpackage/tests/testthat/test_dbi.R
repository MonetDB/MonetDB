library(testthat)
library(MonetDB)
library(MonetDB.R)

tname <- "monetdbtest"
data(iris)
tsize <- function(conn, tname) 
	as.integer(dbGetQuery(conn, paste0("SELECT COUNT(*) FROM ",tname))[[1]])

test_that("db starts up and accepts queries", {
	con <<- dbConnect(MonetDB.R::MonetDB.R(), embedded=tempdir())
	expect_is(con, "MonetDBEmbeddedConnection")
	expect_true(dbIsValid(con))
	res <- dbGetQuery(con, "SELECT 42")
	expect_equal(res$single_value, 42)
	expect_is(res, "data.frame")
})

test_that("raw sql handling", {
	dbSendUpdate(con, "CREATE TABLE monetdbtest (a varchar(10),b integer,c blob)")
	expect_equal(dbExistsTable(con, tname), TRUE)
	dbSendUpdate(con, "INSERT INTO monetdbtest VALUES ('one',1,'1111')")
	dbSendUpdate(con, "INSERT INTO monetdbtest VALUES ('two',2,'22222222')")
	expect_equal(dbGetQuery(con,"SELECT count(*) FROM monetdbtest")[[1]], 2)
	#expect_equal(dbReadTable(con, "monetdbtest")[[3]], list(charToRaw("1111"), charToRaw("22222222")))
	# why does this not work?
	dbRemoveTable(con, tname)
	expect_false(dbExistsTable(con, tname))
})

test_that("import export", {
	data(iris)
	dbWriteTable(con, tname, iris)

	expect_true(dbExistsTable(con, tname))
	expect_false(dbExistsTable(con, "monetdbtest2"))

	expect_true(tname %in% dbListTables(con))
	expect_equal(dbListFields(con, tname), names(iris))

	iris2 <- dbReadTable(con, tname)
	expect_equal(dim(iris), dim(iris2))

	res <- dbSendQuery(con, "SELECT \"Species\", \"Sepal.Width\" FROM monetdbtest")
	expect_true(dbIsValid(res))
	expect_is(res, "MonetDBEmbeddedResult")
	expect_true(res@env$success)
	expect_equal(dbColumnInfo(res)[[1,1]], "Species")
	expect_equal(dbColumnInfo(res)[[2,1]], "Sepal.Width")
	expect_equal(dbGetInfo(res)$row.count, 150)
	expect_equal(res@env$info$rows, 150)
	expect_error(dbFetch(res,10))

	data2 <- dbFetch(res,-1)
	expect_equal(dim(data2)[[1]], 150)
	expect_true(dbHasCompleted(res))
	expect_true(dbIsValid(res))
	dbClearResult(res)
	expect_false(dbIsValid(res))

	dbRemoveTable(con, tname)
	expect_false(dbExistsTable(con, tname))
	expect_error(dbFetch(res))
})

test_that("csv import", {
	tf <- tempfile()
	on.exit(unlink(tf))

	write.table(iris, tf, sep=",", row.names=FALSE)
	tname2 <- "Need to quote this table name"
	monetdb.read.csv(con, tf, tname)
	monetdb.read.csv(con, tf, tname2)
	expect_true(dbExistsTable(con, tname))
	expect_true(dbExistsTable(con,tname2))

	iris3 <- dbReadTable(con, tname)
	iris4 <- dbReadTable(con, tname2)
	expect_equal(dim(iris), dim(iris3))
	expect_equal(dim(iris), dim(iris4))
	expect_equal(dbListFields(con,tname), names(iris))
	expect_equal(dbListFields(con,tname2), names(iris)) 

	dbRemoveTable(con, tname)
	dbRemoveTable(con, tname2)
	expect_false(dbExistsTable(con, tname))
	expect_false(dbExistsTable(con, tname2))
})

test_that("write table with complications", {
	# make sure table is gone before we start
	if (dbExistsTable(con,tname))
		dbRemoveTable(con,tname)

	# table does not exist, append=F, overwrite=F, this should work
	dbWriteTable(con, tname, mtcars, append=F, overwrite=F)
	expect_true(dbExistsTable(con, tname))
	expect_equal(nrow(mtcars), tsize(con, tname))

	# these should throw errors
	expect_error(dbWriteTable(con, tname, mtcars, append=F, overwrite=F))
	expect_error(dbWriteTable(con ,tname, mtcars, overwrite=T, append=T))

	# this should be fine
	dbWriteTable(con, tname, mtcars, append=F, overwrite=T)
	expect_true(dbExistsTable(con, tname))
	expect_equal(nrow(mtcars), tsize(con, tname))

	# append to existing table
	dbWriteTable(con, tname, mtcars, append=T, overwrite=F)
	expect_equal(as.integer(2*nrow(mtcars)), tsize(con, tname))
	dbRemoveTable(con, tname)
	expect_false(dbExistsTable(con, tname))

	# use inserts
	dbWriteTable(con, tname, mtcars, append=F, overwrite=F, insert=T)
	expect_true(dbExistsTable(con, tname))
	expect_equal(nrow(mtcars), tsize(con, tname))
	dbRemoveTable(con, tname)
	expect_false(dbExistsTable(con, tname))
})

test_that("transactions", {
	dbSendQuery(con, "CREATE TABLE monetdbtest (a INTEGER)")
	expect_true(dbExistsTable(con ,tname))
	dbBegin(con)
	dbSendQuery(con, "INSERT INTO monetdbtest VALUES (42)")
	expect_equal(1, tsize(con, tname))
	dbRollback(con)
	expect_equal(0, tsize(con, tname))
	dbBegin(con)
	dbSendQuery(con, "INSERT INTO monetdbtest VALUES (42)")
	expect_equal(1, tsize(con, tname))
	dbCommit(con)
	expect_equal(1, tsize(con, tname))
	dbRemoveTable(con, tname)
	expect_false(dbExistsTable(con, tname))
})

test_that("funny characters", {
	dbBegin(con)
	dbSendQuery(con, "CREATE TABLE monetdbtest (a string)")
	dbSendQuery(con, "INSERT INTO monetdbtest VALUES ('Роман Mühleisen')")
	expect_equal("Роман Mühleisen", dbGetQuery(con, "SELECT a FROM monetdbtest")$a[[1]])
	dbSendQuery(con, "DELETE FROM monetdbtest")
	dbSendUpdate(con, "INSERT INTO monetdbtest (a) VALUES (?)", "Роман Mühleisen")
	expect_equal("Роман Mühleisen", dbGetQuery(con, "SELECT a FROM monetdbtest")$a[[1]])
	dbRollback(con)
})

test_that("esoteric type MONTH_INTERVAL", {
	expect_equal(1L, as.integer(dbGetQuery(con, "select cast('2015-03-02' as date) - cast('2015-03-01' as date)")[[1]][[1]]))
})

test_that("reserved words in df col names", {
	dbBegin(con)
	dbWriteTable(con, tname, data.frame(year=42, month=12, day=24, some.dot=12), transaction=F)
	expect_true(dbExistsTable(con, tname))
	dbRollback(con)
})

test_that("empty result set", {
	expect_true(!is.null(dbGetQuery(con, "SELECT * FROM tables WHERE 1=0")))
})

test_that("evil table from survey works", {
	dbBegin(con)
	data(api, package="survey")
	x <- apiclus1
	x$idkey <- seq(nrow(x))
	dbWriteTable(con, tname, x, transaction=F)
	expect_true(dbExistsTable(con, tname))
	dbRollback(con)
})

test_that("dis/re-connect", {
	expect_true(dbIsValid(con))
	dbDisconnect(con)
	expect_false(dbIsValid(con))
	expect_error(dbSendQuery(con, "SELECT 1"))
	# this throws a warning because we cannot re-initialize embedded monetdb
	expect_warning(con <- dbConnect(MonetDB.R::MonetDB.R(), embedded=tempdir()))
	res <- dbSendQuery(con, "SELECT 1")
	expect_true(dbIsValid(res))
})
