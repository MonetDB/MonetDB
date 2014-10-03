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

options(monetdb.insert.splitsize=10)
tname <- "monetdbtest"


drv <- dbDriver("MonetDB")
stopifnot(identical(dbGetInfo(drv)$name,"MonetDBDriver"))

con <- conn <- dbConnect(drv, port=dbport, dbname=dbname)
stopifnot(identical(class(con)[[1]],"MonetDBConnection"))

# basic MAPI/SQL test
stopifnot(identical(dbGetQuery(con,"SELECT 'DPFKG!'")[[1]],"DPFKG!"))

# is valid?
stopifnot(dbIsValid(con))
stopifnot(dbIsValid(drv))

# remove test table
if (dbExistsTable(con,tname)) dbRemoveTable(con,tname)
stopifnot(identical(dbExistsTable(con,tname),FALSE))


# test raw handling
dbSendUpdate(con,"CREATE TABLE monetdbtest (a varchar(10),b integer,c blob)")
stopifnot(identical(dbExistsTable(con,tname),TRUE))
dbSendUpdate(con,"INSERT INTO monetdbtest VALUES ('one',1,'1111')")
dbSendUpdate(con,"INSERT INTO monetdbtest VALUES ('two',2,'22222222')")
stopifnot(identical(dbGetQuery(con,"SELECT count(*) FROM monetdbtest")[[1]],2))
stopifnot(identical(dbReadTable(con,tname)[[3]],list(charToRaw("1111"),charToRaw("22222222"))))
dbRemoveTable(con,tname)
stopifnot(identical(dbExistsTable(con,tname),FALSE))

# write test table iris
data(iris)
dbWriteTable(con,tname,iris)

stopifnot(identical(dbExistsTable(con,tname),TRUE))
stopifnot(identical(dbExistsTable(con,"monetdbtest2"),FALSE))
stopifnot(tname %in% dbListTables(con))

stopifnot(identical(dbListFields(con,tname),c("sepal_length","sepal_width",
	"petal_length","petal_width","species")))
# get stuff, first very convenient
iris2 <- dbReadTable(con,tname)
stopifnot(identical(dim(iris),dim(iris2)))


# then manually
res <- dbSendQuery(con,"SELECT species, sepal_width FROM monetdbtest")
stopifnot(dbIsValid(res))
stopifnot(identical(class(res)[[1]],"MonetDBResult"))
stopifnot(identical(res@env$success,TRUE))

stopifnot(dbColumnInfo(res)[[1,1]] == "species")
stopifnot(dbColumnInfo(res)[[2,1]] == "sepal_width")

stopifnot(dbGetInfo(res)$row.count == 150 && res@env$info$rows == 150)

data <- dbFetch(res,10)

stopifnot(dim(data)[[1]] == 10)
stopifnot(dim(data)[[2]] == 2)
stopifnot(res@env$delivered == 10)
stopifnot(dbHasCompleted(res) == FALSE)

data2 <- dbFetch(res,-1)
stopifnot(dim(data2)[[1]] == 140)
stopifnot(dbHasCompleted(res) == TRUE)

stopifnot(dbIsValid(res))
dbClearResult(res)
stopifnot(!dbIsValid(res))

# remove table again
dbRemoveTable(con,tname)
stopifnot(identical(dbExistsTable(con,tname),FALSE))

# test csv import
file <- tempfile()
write.table(iris,file,sep=",")
monetdb.read.csv(con,file,tname,150)
unlink(file)
stopifnot(identical(dbExistsTable(con,tname),TRUE))
iris3 <- dbReadTable(con,tname)
stopifnot(identical(dim(iris),dim(iris3)))
stopifnot(identical(dbListFields(con,tname),c("sepal_length","sepal_width",
	"petal_length","petal_width","species")))
dbRemoveTable(con,tname)
stopifnot(identical(dbExistsTable(con,tname),FALSE))

# test dbWriteTable
tsize <- function(conn,tname) 
	as.integer(dbGetQuery(conn,paste0("SELECT COUNT(*) FROM ",tname))[[1]])

# clean up
if (dbExistsTable(conn,tname))
	dbRemoveTable(conn,tname)

# table does not exist, append=F, overwrite=F, this should work
dbWriteTable(conn,tname,mtcars,append=F,overwrite=F)
stopifnot(dbExistsTable(conn,tname))
stopifnot(identical(nrow(mtcars),tsize(conn,tname)))

# these should throw errors
errorThrown <- F
tryCatch(dbWriteTable(conn,tname,mtcars,append=F,overwrite=F),error=function(e){errorThrown <<- T})
stopifnot(errorThrown)

errorThrown <- F
tryCatch(dbWriteTable(conn,tname,mtcars,overwrite=T,append=T),error=function(e){errorThrown <<- T})
stopifnot(errorThrown)

# this should be fine
dbWriteTable(conn,tname,mtcars,append=F,overwrite=T)
stopifnot(dbExistsTable(conn,tname))
stopifnot(identical(nrow(mtcars),tsize(conn,tname)))

# append to existing table
dbWriteTable(conn,tname,mtcars,append=T,overwrite=F)
stopifnot(identical(as.integer(2*nrow(mtcars)),tsize(conn,tname)))
dbRemoveTable(conn,tname)

dbRemoveTable(conn,tname)
dbWriteTable(conn,tname,mtcars,append=F,overwrite=F,insert=T)
dbRemoveTable(conn,tname)

# info
stopifnot(identical("MonetDBDriver", dbGetInfo(MonetDB.R())$name))
stopifnot(identical("MonetDBConnection", dbGetInfo(conn)$name))

# transactions...
sq <- dbSendQuery(conn,"create table monetdbtest (a integer)")
stopifnot(dbExistsTable(conn,tname))
dbBegin(conn)
sq <- dbSendQuery(conn,"INSERT INTO monetdbtest VALUES (42)")
stopifnot(identical(1L, tsize(conn, tname)))
dbRollback(conn)
stopifnot(identical(0L, tsize(conn, tname)))
dbBegin(conn)
sq <- dbSendQuery(conn,"INSERT INTO monetdbtest VALUES (42)")
stopifnot(identical(1L, tsize(conn, tname)))
dbCommit(conn)
stopifnot(identical(1L, tsize(conn, tname)))
dbRemoveTable(conn,tname)

stopifnot(dbIsValid(conn))
#thrice to catch null pointer errors
stopifnot(identical(dbDisconnect(con),TRUE))
stopifnot(!dbIsValid(conn))
stopifnot(identical(dbDisconnect(con),TRUE))
stopifnot(identical(dbDisconnect(con),TRUE))

#test merovingian control code
#cannot really do this in Mtest, sorry
#stopifnot(dbname %in% monetdbd.liststatus("monetdb")$dbname)

print("SUCCESS")
