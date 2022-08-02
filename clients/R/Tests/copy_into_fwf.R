if (Sys.getenv("TSTTRGDIR") != "") {
	.libPaths(c(.libPaths(), paste0(Sys.getenv("TSTTRGDIR"),"/rlibdir")))
}
library(DBI, quietly = T)

args <- commandArgs(trailingOnly = TRUE)
dbport <- 50000
dbname <- "mTests_clients_R"
if (length(args) > 0) 
	dbport <- args[[1]]
if (length(args) > 1) 
	dbname <- args[[2]]

con <- dbConnect(MonetDBLite::MonetDB(), port = dbport, dbname = dbname, wait = T)
stopifnot(dbIsValid(con))

tf <- tempfile()

gdata::write.fwf(mtcars, tf, colnames = FALSE)

if (dbExistsTable(con, "mtcars")) dbRemoveTable(con, "mtcars")

dbBegin(con)
dbSendQuery(con, "CREATE TABLE mtcars (mpg DOUBLE PRECISION, cyl DOUBLE PRECISION, disp DOUBLE PRECISION, hp DOUBLE PRECISION, drat DOUBLE PRECISION, wt DOUBLE PRECISION, qsec DOUBLE PRECISION, vs DOUBLE PRECISION, am DOUBLE PRECISION, gear DOUBLE PRECISION, carb DOUBLE PRECISION)")

# delimiters are ineffective for fwf import just set them to make sure they dont break stuff
res <- dbSendQuery(con, paste0("COPY OFFSET 1 INTO mtcars FROM '", tf, "' USING DELIMITERS 'a','b','c' NULL AS '' FWF (4, 2, 6, 4, 5, 6, 6, 2, 2, 2, 2)"))

print(dbReadTable(con, "mtcars"))

stopifnot(nrow(dbReadTable(con, "mtcars")) > 1)

dbRollback(con)

print("SUCCESS")
