# Mtest boilerplate
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

db <- dbConnect(MonetDB(), port=dbport, dbname=dbname, wait=T)
stopifnot(dbIsValid(db))
# End Mtest boilerplate

out <- NULL
for ( i in 1:100 ){
	out <- c(out, dbGetQuery(db , "SELECT * FROM (SELECT 1 AS col UNION ALL SELECT 2 AS col) AS temp SAMPLE 0.5"))
}
dbDisconnect(db)

tt <- table(as.integer(out))
stopifnot(abs(1 - tt[[1]]/tt[[2]]) < 0.4)

print("SUCCESS")
