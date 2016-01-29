ll <- NULL
if (Sys.getenv("TSTTRGDIR") != "") {
	ll <- paste0(Sys.getenv("TSTTRGDIR"),"/rlibdir")
}
suppressMessages({
	library(MonetDB.R,quietly=T,lib.loc=ll)
	library(sqlsurvey,quietly=T)
})

args <- commandArgs(trailingOnly = TRUE)
dbport <- 50000
dbname <- "mTests_clients_R"
if (length(args) > 0) 
	dbport <- args[[1]]
if (length(args) > 1) 
	dbname <- args[[2]]

# install.packages("sqlsurvey", repos=c("http://cran.r-project.org","http://R-Forge.R-project.org"), dep=TRUE)

dburl <- paste0("monetdb://localhost:",dbport,"/",dbname)
options(monetdb.profile=F)
db <- dbConnect( MonetDB.R() , dburl)

data( api )

x <- apiclus1
x$idkey <- 1:nrow( x )

# monetdb doesn't like the column name `full`
x$full <- NULL

names(x) <- tolower(gsub(".","_",names(x),fixed=T))

# load the apiclus1 data set into the monetdb
dbWriteTable( db , 'apiclus1' , x , overwrite = TRUE )
cat("#~BeginVariableOutput~#\n")

dclus1 <-
		sqlsurvey(
				weight = 'pw' ,
				id = 'dnum' ,
				# fpc = 'fpc' ,
				table.name = 'apiclus1' ,
				key = "idkey" ,
				database = dburl ,
				driver = MonetDB.R() ,
				user = "monetdb" ,
				password = "monetdb" 
		)

# only a problem for factor variables..

# these all work
svymean( ~dname , dclus1 )
svymean( ~dname , dclus1 , se = TRUE )
svymean( ~dname , dclus1 , byvar = ~comp_imp )
# then this breaks!
svymean( ~dname , dclus1 , byvar = ~comp_imp , se = TRUE )

# ..and now these same three commands no longer work!
svymean( ~dname , dclus1 )
svymean( ~dname , dclus1 , se = TRUE )
svymean( ~dname , dclus1 , byvar = ~comp_imp )

# but then actual queries do work
dbGetQuery( db , 'select * from apiclus1 limit 2' )
dbRemoveTable(db,"apiclus1")
cat("#~EndVariableOutput~#\n")


print("SUCCESS")
dbDisconnect(db)
