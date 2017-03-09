library(DBI)

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


drv <- MonetDBLite::MonetDB()
stopifnot(identical(dbGetInfo(drv)$name, "MonetDBDriver"))

con <- conn <- dbConnect(drv, port=dbport, dbname=dbname, wait=T)
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
MonetDBLite::dbSendUpdate(con,"CREATE TABLE monetdbtest (a varchar(10),b integer,c blob)")
stopifnot(identical(dbExistsTable(con,tname),TRUE))
MonetDBLite::dbSendUpdate(con,"INSERT INTO monetdbtest VALUES ('one',1,'1111')")
MonetDBLite::dbSendUpdate(con,"INSERT INTO monetdbtest VALUES ('two',2,'22222222')")
stopifnot(identical(dbGetQuery(con,"SELECT count(*) FROM monetdbtest")[[1]],2))
stopifnot(identical(dbReadTable(con,tname)[[3]],list(charToRaw("1111"),charToRaw("22222222"))))
dbRemoveTable(con,tname)
stopifnot(identical(dbExistsTable(con,tname),FALSE))

# write test table iris
# iris is one of the built-in datasets in R
data(iris)
dbWriteTable(con,tname,iris)

stopifnot(identical(dbExistsTable(con,tname),TRUE))
stopifnot(identical(dbExistsTable(con,"monetdbtest2"),FALSE))
stopifnot(tname %in% dbListTables(con))
stopifnot(identical(dbListFields(con,tname),names(iris)))
# get stuff, first very convenient
iris2 <- dbReadTable(con,tname)
stopifnot(identical(dim(iris),dim(iris2)))


# then manually
res <- dbSendQuery(con,"SELECT \"Species\", \"Sepal.Width\" FROM monetdbtest")
stopifnot(dbIsValid(res))
stopifnot(MonetDBLite::isIdCurrent(res))
stopifnot(identical(class(res)[[1]],"MonetDBResult"))
stopifnot(identical(res@env$success,TRUE))

stopifnot(dbColumnInfo(res)[[1,1]] == "Species")
stopifnot(dbColumnInfo(res)[[2,1]] == "Sepal.Width")

stopifnot(dbGetRowCount(res) == 0)

data <- dbFetch(res,10)

stopifnot(dbGetRowCount(res) == 10)


stopifnot(dim(data)[[1]] == 10)
stopifnot(dim(data)[[2]] == 2)
stopifnot(res@env$delivered == 10)
stopifnot(dbHasCompleted(res) == FALSE)

data2 <- dbFetch(res,-1)

stopifnot(dbGetRowCount(res) == 150)

stopifnot(dim(data2)[[1]] == 140)
stopifnot(dbHasCompleted(res) == TRUE)

stopifnot(dbIsValid(res))
dbClearResult(res)
stopifnot(!dbIsValid(res))

# remove table again
dbRemoveTable(con,tname)
stopifnot(identical(dbExistsTable(con,tname),FALSE))

# test csv import
tf <- tempfile()
write.table(iris,tf,sep=",",row.names=FALSE)
tname2 <- "Need to quote this table name"
tname3 <- "othermethod"
MonetDBLite::monetdb.read.csv(con,tf,tname)
MonetDBLite::monetdb.read.csv(con,tf,tname2)
dbWriteTable(con, tname3, tf)

###
dbListTables(con)

unlink(tf)
stopifnot(identical(dbExistsTable(con,tname),TRUE))
stopifnot(identical(dbExistsTable(con,tname2),TRUE))
stopifnot(identical(dbExistsTable(con,tname3),TRUE))

iris3 <- dbReadTable(con,tname)
iris4 <- dbReadTable(con,tname2)
iris5 <- dbReadTable(con,tname3)
stopifnot(identical(dim(iris),dim(iris3)))
stopifnot(identical(dim(iris),dim(iris4)))
stopifnot(identical(dim(iris),dim(iris5)))
stopifnot(identical(dbListFields(con,tname),names(iris)))
stopifnot(identical(dbListFields(con,tname2),names(iris)))
stopifnot(identical(dbListFields(con,tname3),names(iris)))

dbRemoveTable(con,tname)
dbRemoveTable(con,tname2)
dbRemoveTable(con,tname3)

stopifnot(identical(dbExistsTable(con,tname),FALSE))
stopifnot(identical(dbExistsTable(con,tname2),FALSE))
stopifnot(identical(dbExistsTable(con,tname3),FALSE))

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

dbWriteTable(conn,tname,mtcars,append=F,overwrite=F,insert=T)
dbRemoveTable(conn,tname)

# info
stopifnot(identical("MonetDBDriver", dbGetInfo(MonetDBLite::MonetDB.R())$name))
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

# funny characters in strings
stopifnot(dbIsValid(conn))
dbBegin(conn)
sq <- dbSendQuery(conn,"CREATE TABLE monetdbtest (a string)")
sq <- dbSendQuery(conn,"INSERT INTO monetdbtest VALUES ('Роман Mühleisen')")
stopifnot(identical("Роман Mühleisen", dbGetQuery(conn,"SELECT a FROM monetdbtest")$a[[1]]))
sq <- dbSendQuery(conn,"DELETE FROM monetdbtest")
MonetDBLite::dbSendUpdate(conn, "INSERT INTO monetdbtest (a) VALUES (?)", "Роман Mühleisen")
stopifnot(identical("Роман Mühleisen", dbGetQuery(conn,"SELECT a FROM monetdbtest")$a[[1]]))
dbRollback(conn)

# this returns a column with esoteric type MONTH_INTERVAL
stopifnot(identical(1L, as.integer(dbGetQuery(con, "select cast('2015-03-02' as date) - cast('2015-03-01' as date)")[[1]][[1]])))

# reserved words in data frame column names
stopifnot(dbIsValid(conn))
dbBegin(conn)
dbWriteTable(conn, "evilt", data.frame(year=42, month=12, day=24, some.dot=12), transaction=F)
stopifnot(dbExistsTable(conn, "evilt"))
dbRollback(conn)

# evil table from survey
stopifnot(dbIsValid(conn))
dbBegin(conn)
data(api, package="survey")
x <- apiclus1
x$idkey <- seq( nrow( x ) )
dbWriteTable( conn , 'x' , x , transaction=F)
stopifnot(dbExistsTable(conn, "x"))
dbRollback(conn)

# empty result set
stopifnot(!is.null(dbGetQuery(conn, "SELECT * FROM tables WHERE 1=0")))

#non-standard dbwritetable use
dbBegin(conn)
dbWriteTable(conn, "vectable", 1:1000, transaction=F)
stopifnot(dbExistsTable(conn, "vectable"))
dbRollback(conn)

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
