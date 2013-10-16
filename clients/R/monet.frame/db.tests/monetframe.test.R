library(MonetDB.R)

con <- dbConnect(dbDriver("MonetDB"), "monetdb://localhost:50000/acs", "monetdb", "monetdb",timeout=100)

table <- "monetframetest"


fcmp <- function(f1,f2,epsilon) {
	abs(f1-f2) < epsilon
}

# basic MAPI/SQL test
stopifnot(identical(dbGetQuery(con,"SELECT 'DPFKG!'")[[1]],"DPFKG!"))

if (!dbExistsTable(con,table)) {
	data(iris)
	dbWriteTable(con,table,iris, overwrite=TRUE)
}
stopifnot(identical(dbExistsTable(con,table),TRUE))

# aight
frame <- monet.frame(con,table)
stopifnot(identical(class(frame)[[1]],"monet.frame"))

# we should get the very same from monet.frame and dbReadTable
plaindata <- dbReadTable(con,table)
stopifnot(identical(as.data.frame(frame),plaindata))

# do as.vector / $ work?
stopifnot(identical(as.vector(frame$sepal_width),plaindata$sepal_width))

# does [] work?
stopifnot(identical(as.data.frame(frame[1:10,c("sepal_length","species")]),plaindata[1:10,c("sepal_length","species")]))

# names(), dim()
stopifnot(identical(names(frame),c("sepal_length","sepal_width","petal_length","petal_width","species")))
stopifnot(dim(frame)[[1]] == 150)
stopifnot(dim(frame)[[2]] == 5)

# Ops
stopifnot(identical(plaindata$sepal_width*plaindata$sepal_length,as.vector(frame$sepal_width*frame$sepal_length)))
stopifnot(identical(plaindata$sepal_width*42,as.vector(frame$sepal_width*42)))
stopifnot(identical(42*plaindata$sepal_length,as.vector(42*frame$sepal_length)))
stopifnot(identical(plaindata$sepal_length > 1.4,as.vector(frame$sepal_length > 1.4)))

# Summary
stopifnot(identical(min(plaindata$sepal_length),min(frame$sepal_length)))
stopifnot(identical(max(plaindata$sepal_length),max(frame$sepal_length)))
stopifnot(identical(round(mean(plaindata$sepal_length),2),round(mean(frame$sepal_length),2)[[1]]))

# Math
stopifnot(identical(signif(plaindata$sepal_length*1000,2),as.vector(signif(frame$sepal_length * 1000,2))))


# subset
# have to realign row numbers for compat.
sdf <- subset(plaindata,sepal_width > 3 & species == "setosa")
rownames(sdf) <- 1:nrow(sdf)
smf <- as.data.frame(subset(frame,sepal_width > 3 & species == "setosa"))
stopifnot(identical(sdf,smf))

# moar ops
stopifnot(fcmp(
	sd(plaindata$sepal_width),
	sd(frame$sepal_width)
,0.1))

stopifnot(fcmp(
	var(plaindata$sepal_width),
	var(frame$sepal_width)
,0.1))

dbRemoveTable(con,table)
dbDisconnect(con)
print("SUCCESS")