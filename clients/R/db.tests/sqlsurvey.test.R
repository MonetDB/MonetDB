library(MonetDB.R)
library(sqlsurvey)

# install.packages("sqlsurvey", repos=c("http://cran.r-project.org","http://R-Forge.R-project.org"), dep=TRUE)

db <- dbConnect( MonetDB.R() , "monetdb://localhost/monetdbrtest")

data( api )

x <- apiclus1
x$idkey <- 1:nrow( x )

# monetdb doesn't like the column name `full`
x$full <- NULL

# load the apiclus1 data set into the monetdb
dbWriteTable( db , 'apiclus1' , x , overwrite = TRUE )

dclus1 <-
		sqlsurvey(
				weight = 'pw' ,
				id = 'dnum' ,
				# fpc = 'fpc' ,
				table.name = 'apiclus1' ,
				key = "idkey" ,
				database = "monetdb://localhost/monetdbrtest" ,
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


print("SUCCESS")
