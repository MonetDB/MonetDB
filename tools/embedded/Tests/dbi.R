basedir <- Sys.getenv("TSTTRGDIR")
if (basedir != "") {
	library(MonetDBLite, quietly=T, lib.loc=file.path(basedir, "rlibdir"))
} else {
	library(MonetDBLite)
}
library(DBI)
library(MonetDB.R)
library(testthat)
dbdir <- tempdir()
tname <- "monetdbtest"
data(iris)
tsize <- function(conn, tname) 
	as.integer(dbGetQuery(conn, paste0("SELECT COUNT(*) FROM ",tname))[[1]])

test_that("db starts up and accepts queries", {
	con <<- dbConnect(MonetDBLite(), dbdir)
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
	expect_equal(dbGetRowCount(res), 150)
	expect_equal(res@env$info$rows, 150)

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

# below some DBI test cases 'borrowed' from RSQLite
basicDf <- data.frame(
	name = c("Alice", "Bob", "Carl", "NA", NA),
	fldInt = as.integer(c(as.integer(1:4), NA)),
	fldDbl = as.double(c(1.1, 2.2, 3.3, 4.4, NA)),
	stringsAsFactors = FALSE
)

test_that("round-trip leaves data.frame unchanged", {
	dbWriteTable(con, "t1", basicDf, row.names = FALSE)
	expect_equivalent(dbGetQuery(con, "select * from t1"), basicDf)
	expect_equivalent(dbReadTable(con, "t1"), basicDf)
	dbRemoveTable(con, "t1")
})

test_that("NAs work in first row", {
	na_first <- basicDf[c(5, 1:4), ]
	rownames(na_first) <- NULL
	dbWriteTable(con, "t1", na_first, row.names = FALSE)
	expect_equivalent(dbReadTable(con, "t1"), na_first)
	dbRemoveTable(con, "t1")
})

test_that("row-by-row fetch is equivalent", {
	dbWriteTable(con, "t1", basicDf, row.names = FALSE)
	rs <- dbSendQuery(con, "SELECT * FROM t1")
	on.exit(dbClearResult(rs))
	for (i in 1:5) {
		row <- dbFetch(rs, 1L)
		expect_equal(row, basicDf[i, ], check.attributes = FALSE)
	}
	row <- dbFetch(rs, 1L)
	expect_equal(nrow(row), 0L)
	expect_true(dbHasCompleted(rs))
	dbRemoveTable(con, "t1")
})

test_that("column types as expected in presence of NULLs", {
	dbWriteTable(con, "t1", datasets::USArrests)
	expect_error( dbGetQuery(con, "SELECT \"Murder\"/(\"Murder\" - 8.1) FROM t1 LIMIT 10"))
  	dbRemoveTable(con, "t1")
})

test_that("correct number of columns, even if 0 rows", {
	ans <- dbGetQuery(con, "select 1 as a, 2 as b where 1=1")
	expect_equal(dim(ans), c(1L, 2L))
	ans <- dbGetQuery(con, "select 1 as a, 2 as b where 0=1")
	expect_equal(dim(ans), c(0L, 2L))  
})

test_that("accessing cleared result throws error", {  
	res <- dbSendQuery(con, "SELECT 1;")
	dbClearResult(res)
	expect_error(dbFetch(res))
})

test_that("fetch with no arguments gets all rows", {
	df <- data.frame(x = 1:1000)
	dbWriteTable(con, "test", df)
	rs <- dbSendQuery(con, "SELECT * FROM test")
	expect_equal(nrow(dbFetch(rs)), 1000)
	dbRemoveTable(con, "test")
})

test_that("fetch progressively pulls in rows", {  
	df <- data.frame(x = 1:25)
	dbWriteTable(con, "test", df)
	rs <- dbSendQuery(con, "SELECT * FROM test")
	expect_equal(nrow(dbFetch(rs, 10)), 10)
	expect_equal(nrow(dbFetch(rs, 10)), 10)
	expect_equal(nrow(dbFetch(rs, 10)), 5)
	dbRemoveTable(con, "test")
})

test_that("this never happens again", {
	dbBegin(con)
	dbSendQuery(con, "create table ep (pde_id clob, bene_id clob, srvc_dt clob, prod_srvc_id clob, plan_cntrct_rec_id clob, plan_pbp_rec_num clob, qty_dspnsd_num double, days_suply_num double, drug_cvrg_stus_cd clob, ctstrphc_cvrg_cd clob, ptnt_pay_amt double, othr_troop_amt double, lics_amt double, plro_amt double, cvrd_d_plan_pd_amt double, ncvrd_plan_pd_amt double, tot_rx_cst_amt double, benefit_phase clob, ccw_pharmacy_id double, ccw_prscrbr_id double, pde_prscrbr_id_frmt_cd clob, formulary_id clob, frmlry_rx_id clob, rptd_gap_dscnt_num double, brnd_gnrc_cd clob, rx_c_cov double, rx_c_lics double, rx_c_tpc double, rx_c_tdc double, rx_c_oop double, rx_c_gdc double, rx_e_cov double, rx_e_lics double, rx_e_tpc double, rx_e_tdc double, rx_e_oop double, rx_e_gdc double, rx_o_cov double, rx_o_lics double, rx_o_tpc double, rx_o_tdc double, rx_o_oop double, rx_o_gdc double, rx_ce_cov double, rx_ce_lics double, rx_ce_tpc double, rx_ce_tdc double, rx_ce_oop double, rx_ce_gdc double, rx_co_cov double, rx_co_lics double, rx_co_tpc double, rx_co_tdc double, rx_co_oop double, rx_co_gdc double, rx_eo_cov double, rx_eo_lics double, rx_eo_tpc double, rx_eo_tdc double, rx_eo_oop double, rx_eo_gdc double, rx_ceo_cov double, rx_ceo_lics double, rx_ceo_tpc double, rx_ceo_tdc double, rx_ceo_oop double, rx_ceo_gdc double);
create table aep13 as select bene_id ,CAST( COUNT( DISTINCT CASE WHEN drug_cvrg_stus_cd IN ('c') THEN ccw_prscrbr_id END ) AS DOUBLE ) as rx_c_pre,CAST( COUNT( DISTINCT CASE WHEN drug_cvrg_stus_cd IN ('c') THEN ccw_pharmacy_id END ) AS DOUBLE ) as rx_c_pha from ep group by bene_id WITH DATA;")
		dbRollback(con)
	})

test_that("dis/re-connect", {
	expect_true(dbIsValid(con))
	dbDisconnect(con)
	expect_false(dbIsValid(con))
	expect_error(dbSendQuery(con, "SELECT 1"))
	# we can restart in same dir
	con <- dbConnect(MonetDB.R::MonetDB.R(), embedded=dbdir)
	expect_true(dbIsValid(con))
	res <- dbSendQuery(con, "SELECT 1")
	expect_true(dbIsValid(res))
	# but not in a different one
	expect_error(con <- dbConnect(MonetDBLite(), "/tmp"))
})

