ll <- NULL
if (Sys.getenv("TSTTRGDIR") != "") {
	ll <- paste0(Sys.getenv("TSTTRGDIR"),"/rlibdir")
}
library(MonetDB.R,quietly=T,lib.loc=ll)

args <- commandArgs(trailingOnly = TRUE)
dbport <- 50000
dbname <- "mTests_clients_R"
if (length(args) > 0) 
	dbport <- args[[1]]
if (length(args) > 1) 
	dbname <- args[[2]]

options(monetdb.insert.splitsize=10)
options(monetdb.profile=F)


tname <- "monetdbtest"

con <- dbConnect(MonetDB(), port=dbport, dbname=dbname, wait=T)
stopifnot(dbIsValid(con))

#options(monetdb.debug.query=T)
# make sure embedded R is working in general
dbBegin(con)
invisible(dbSendQuery(con, "CREATE FUNCTION fuuu() RETURNS TABLE(i INTEGER) LANGUAGE R {42L}"))
res <- dbGetQuery(con, "SELECT * FROM fuuu();")
stopifnot(identical(42L, res$i[[1]]))
dbRollback(con)

data(mtcars)
dbWriteTable(con,tname,mtcars, overwrite=T)
stopifnot(identical(TRUE, dbExistsTable(con,tname)))

res <- dbApply(con, tname, function(d) {
	d$mpg
})
stopifnot(identical(res, mtcars$mpg))

res <- dbApply(con, tname, function(d) {
	min(d$mpg)
})
stopifnot(identical(res, min(mtcars$mpg)))

# model fitting / in-db application
fitted <- lm(mpg~.,data=mtcars) 
predictions <- dbApply(con,tname,function(d) {
  predict(fitted, newdata=d)
})

stopifnot(identical(unname(predict(fitted, newdata=mtcars)), unname(predictions)))

dbRemoveTable(con,tname)
stopifnot(identical(FALSE, dbExistsTable(con,tname)))



print("SUCCESS")
