options(monetdb.debug.query=T)

library(MonetDB.R)

drv <- dbDriver("MonetDB")
stopifnot(identical(dbGetInfo(drv)$name,"MonetDBDriver"))

con <- dbConnect(drv, "monetdbrtest")
stopifnot(identical(class(con)[[1]],"MonetDBConnection"))
# overwrite variable to force destructor
con <- dbConnect(drv, "monetdbrtest")
con <- dbConnect(drv, "monetdbrtest")
gc()

# basic MAPI/SQL test
stopifnot(identical(dbGetQuery(con,"SELECT 'DPFKG!'")[[1]],"DPFKG!"))

# remove test table
if (dbExistsTable(con,"monetdbtest")) dbRemoveTable(con,"monetdbtest")
stopifnot(identical(dbExistsTable(con,"monetdbtest"),FALSE))


# test raw handling
dbSendUpdate(con,"CREATE TABLE monetdbtest (a varchar(10),b integer,c blob)")
stopifnot(identical(dbExistsTable(con,"monetdbtest"),TRUE))
dbSendUpdate(con,"INSERT INTO monetdbtest VALUES ('one',1,'1111')")
dbSendUpdate(con,"INSERT INTO monetdbtest VALUES ('two',2,'22222222')")
stopifnot(identical(dbGetQuery(con,"SELECT count(*) FROM monetdbtest")[[1]],2))
stopifnot(identical(dbReadTable(con,"monetdbtest")[[3]],list(charToRaw("1111"),charToRaw("22222222"))))
stopifnot(identical(as.character(dbListTables(con)),"monetdbtest"))

dbRemoveTable(con,"monetdbtest")
stopifnot(identical(dbExistsTable(con,"monetdbtest"),FALSE))

# write test table iris
data(iris)
dbWriteTable(con,"monetdbtest",iris)

stopifnot(identical(dbExistsTable(con,"monetdbtest"),TRUE))
stopifnot(identical(dbExistsTable(con,"monetdbtest2"),FALSE))
stopifnot("monetdbtest" %in% dbListTables(con))

stopifnot(identical(dbListFields(con,"monetdbtest"),c("sepal_length","sepal_width","petal_length","petal_width","species")))
# get stuff, first very convenient
iris2 <- dbReadTable(con,"monetdbtest")
stopifnot(identical(dim(iris),dim(iris2)))


# then manually
res <- dbSendQuery(con,"SELECT species, sepal_width FROM monetdbtest")
stopifnot(identical(class(res)[[1]],"MonetDBResult"))
stopifnot(identical(res@env$success,TRUE))

stopifnot(dbColumnInfo(res)[[1,1]] == "species")
stopifnot(dbColumnInfo(res)[[2,1]] == "sepal_width")

stopifnot(dbGetInfo(res)$row.count == 150 && res@env$info$rows == 150)

data <- fetch(res,10)
stopifnot(dim(data)[[1]] == 10)
stopifnot(dim(data)[[2]] == 2)
stopifnot(res@env$delivered == 10)
stopifnot(dbHasCompleted(res) == FALSE)

data2 <- fetch(res,-1)
stopifnot(dim(data2)[[1]] == 140)
stopifnot(dbHasCompleted(res) == TRUE)

dbClearResult(res)

# remove table again
dbRemoveTable(con,"monetdbtest")
stopifnot(identical(dbExistsTable(con,"monetdbtest"),FALSE))

# test csv import
file <- tempfile()
write.table(iris,file,sep=",")
monetdb.read.csv(con,file,"monetdbtest",150)
unlink(file)
stopifnot(identical(dbExistsTable(con,"monetdbtest"),TRUE))
iris3 <- dbReadTable(con,"monetdbtest")
stopifnot(identical(dim(iris),dim(iris3)))
stopifnot(identical(dbListFields(con,"monetdbtest"),c("sepal_length","sepal_width","petal_length","petal_width","species")))
dbRemoveTable(con,"monetdbtest")
stopifnot(identical(dbExistsTable(con,"monetdbtest"),FALSE))


#thrice to catch null pointer errors
stopifnot(identical(dbDisconnect(con),TRUE))
stopifnot(identical(dbDisconnect(con),TRUE))
stopifnot(identical(dbDisconnect(con),TRUE))

#test merovingian control code
stopifnot("monetdbrtest" %in% monetdbd.liststatus("monetdb")$dbname)

print("SUCCESS")
