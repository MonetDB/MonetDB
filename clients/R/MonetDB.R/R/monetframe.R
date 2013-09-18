# this wraps a sql database (in particular MonetDB) with a DBI connector 
# to have it appear like a data.frame

# shorthand constructor, also creates connection to db
mf <- function(database,table,host="localhost",port=50000,user="monetdb",pass="monetdb",debug=FALSE,timeout=100) {
	dburl <- paste0("monetdb://",host,":",port,"/",database)	
	con <- dbConnect(MonetDB.R(), dburl,user,pass,timeout=timeout)
	monet.frame(con,table,debug)
}

# can either be given a query or simply a table name
# now supports hints on table structure to speed up initialization
monet.frame <-  monetframe <- function(conn,tableOrQuery,debug=FALSE) monet.frame.internal(conn,tableOrQuery,debug)

monet.frame.internal <- function(conn,tableOrQuery,debug=FALSE,rtypes.hint=NA,cnames.hint=NA,ncol.hint=NA,nrow.hint=NA) {
	if(missing(conn)) stop("'conn' must be specified")
	if(missing(tableOrQuery)) stop("a sql query or a table name must be specified")
	
	obj = new.env()
	class(obj) = "monet.frame"
	attr(obj,"conn") <- conn
	query <- tableOrQuery
	
	if (length(grep("^SELECT.*",tableOrQuery,ignore.case=TRUE)) == 0) {
		query <- paste0("SELECT * FROM ",make.db.names(conn,tableOrQuery,allow.keywords=FALSE))
	}
	
	attr(obj,"query") <- query
	attr(obj,"debug") <- debug
	
	if (debug) cat(paste0("QQ: '",query,"'\n",sep=""))	
	# do this here, in case the nrow thing needs it
	coltestquery <- gsub("SELECT (.*?) FROM (.*?) (ORDER|LIMIT|OFFSET).*","SELECT \\1 FROM \\2",query,ignore.case=TRUE)
	
	if (!is.na(cnames.hint) && !is.na(ncol.hint) && !is.na(rtypes.hint)) {
		attr(obj,"cnames") <- cnames.hint
		attr(obj,"ncol") <- ncol.hint
		attr(obj,"rtypes") <- rtypes.hint
		
	} else {
		# strip away things the prepare does not like
		coltestquery <- gsub("SELECT (.*?) FROM (.*?) (ORDER|LIMIT|OFFSET).*","SELECT \\1 FROM \\2",query,ignore.case=TRUE)
		
		# get column names and types from prepare response
		res <- dbGetQuery(conn, paste0("PREPARE ",coltestquery))
		attr(obj,"cnames") <- res$column
		attr(obj,"ncol") <- length(res$column)
		attr(obj,"rtypes") <- lapply(res$type,monetdbRtype)
		
		if (debug) cat(paste0("II: 'Re-Initializing column info.'\n",sep=""))	
		
	}
	
	if (!is.na(nrow.hint)) {
		attr(obj,"nrow") <- nrow.hint
	}
	else {
		# get result set length by rewriting to count(*), should be much faster
		# temporarily remove offset/limit, as this screws up counting
		counttestquery <- sub("(SELECT )(.*?)( FROM.*)","\\1COUNT(*)\\3",coltestquery,ignore.case=TRUE)
		nrow <- dbGetQuery(conn,counttestquery)[[1,1]] - .getOffset(query)
	
		limit <- .getLimit(query)
		if (limit > 0) nrow <- min(nrow,limit)
		if (nrow < 1) 
			warning(query, " has zero-row result set.")
		
		attr(obj,"nrow") <- nrow
		if (debug) cat(paste0("II: 'Re-Initializing row count.'\n",sep=""))	
		
	}
	return(obj)
}

set.debug <- function(x,debug){
	attr(x,"debug") <- debug
}

.is.debug <- function(x) {
	attr(x,"debug")
}

.element.limit <- 10000000

as.data.frame.monet.frame <- adf <- function(x, row.names, optional, warnSize=TRUE,...) {
	# check if amount of tuples/fields is larger than some limit
	# raise error if over limit and warnSize==TRUE
	if (ncol(x)*nrow(x) > .element.limit && warnSize) 
		stop(paste0("The total number of elements to be loaded is larger than ",.element.limit,". This is probably very slow. Consider dropping columns and/or rows, e.g. using the [] function. If you really want to do this, call as.data.frame() with the warnSize parameter set to FALSE."))
	# get result set object from frame
	if (.is.debug(x)) cat(paste0("EX: '",attr(x,"query"),"'\n",sep=""))	
	
	return(dbGetQuery(attr(x,"conn"),attr(x,"query")))
}

as.vector.monet.frame <- av <- function(x,...) {
	if (ncol(x) != 1)
		stop("as.vector can only be used on one-column monet.frame objects. Consider using $.")
	as.data.frame(x)[[1]]
}

# this is the fun part. this method has infinity ways of being invoked :(
# http://stat.ethz.ch/R-manual/R-patched/library/base/html/Extract.data.frame.html

# TODO: handle negative indices and which() calls. which() like subset!
# TODO: subset calls destroy nrows hint

"[.monet.frame" <- function(x, k, j,drop=TRUE) {	
	nquery <- query <- getQuery(x)
	
	cols <- NA
	rows <- NA
	
	nrow.hint <- nrow(x)
	ncol.hint <- ncol(x)
	cnames.hint <- NA
	rtypes.hint <- NA
	
	# biiig fun with nargs to differentiate d[1,] and d[1]
	# all in the presence of the optional drop argument, yuck!
	args <- nargs()
	if (!missing(drop)) {
		args <- args-1
	}
	if (args == 2 && missing(j)) cols <- k
	if (args == 3 && !missing(j)) cols <- j
	if (args == 3 && !missing(k)) rows <- k
		
	if (length(cols) > 1 || !is.na(cols)) { # get around an error if cols is a vector...
		# if we have a numeric column spec, find the appropriate names
		if (is.numeric(cols)) {
			if (min(cols) < 1 || max(cols) > ncol(x)) 
				stop(paste0("Invalid column specification '",cols,"'. Column indices have to be in range [1,",ncol(x),"].",sep=""))			
			cols <- names(x)[cols]
		}
		if (!all(cols %in% names(x)))
			stop(paste0("Invalid column specification '",cols,"'. Column names have to be in set {",paste(names(x),collapse=", "),"}.",sep=""))			
		
		rtypes.hint <- rTypes(x)[match(cols,names(x)),drop=TRUE]
		ncol.hint <- length(cols)
		cnames.hint <- cols
		
		nquery <- sub("SELECT.+FROM",paste0("SELECT ",paste0(make.db.names(attr(x,"conn"),cols),collapse=", ")," FROM"),query)
	}
	
	if (length(rows) > 1 || !is.na(rows)) { # get around an error if cols is a vector...
		if (min(rows) < 1 || max(rows) > nrow(x)) 
			stop("Invalid row specification. Row indices have to be in range [1,",nrow(x),"].",sep="")			
		
		if (.is.sequential(rows)) {
			# find out whether we already have limit and/or offset set
			# our values are relative to them
	
			offset <- .getOffset(nquery) + min(rows)-1 # offset means skip n rows, but r lower limit includes them
			limit <- max(rows)-min(rows)+1

			# remove old limit/offset from query
			# TODO: is this safe? UNION queries are particularly dangerous, again...
			nquery <- gsub("limit[ ]+\\d+|offset[ ]+\\d+","",nquery,ignore.case=TRUE)
			nquery <- sub(";? *$",paste0(" LIMIT ",.mapiLongInt(limit)," OFFSET ",.mapiLongInt(offset)),nquery,ignore.case=TRUE)
			nrow.hint <- limit
		}
		else 
			warning(paste("row specification has to be sequential, but ",paste(rows,collapse=",")," is not. Try as.data.frame(x)[c(",paste(rows,collapse=","),"),] instead.",sep=""))
	}
	
	# this would be the only case for column selection where the 'drop' parameter has an effect.
	# we have to create a warning, since drop=TRUE is default behaviour and might be expected by users
	if (((!is.na(cols) && length(cols) == 1) || (!is.na(rows) && length(rows) == 1)) && drop) 
		warning("drop=TRUE for one-column or one-row results is not supported. Overriding to FALSE")
	
	# construct and return new monet.frame for rewritten query
	monet.frame.internal(attr(x,"conn"),nquery,.is.debug(x),nrow.hint=nrow.hint,ncol.hint=ncol.hint, cnames.hint=cnames.hint, rtypes.hint=rtypes.hint)
}

.getOffset <- function(query) {
	os <- 0
	osStr <- gsub("(.*offset[ ]+)(\\d+)(.*)","\\2",query,ignore.case=TRUE)
	if (osStr != query) {
		os <- as.numeric(osStr)
	}
	os
}

.getLimit <- function(query) {
	l <- 0
	lStr <- gsub("(.*limit[ ]+)(\\d+)(.*)","\\2",query,ignore.case=TRUE)
	if (lStr != query) {
		l <- as.numeric(lStr)
	}
	l
}

.is.sequential <- function(x, eps=1e-8) {
	if (length(x) && isTRUE(abs(x[1] - floor(x[1])) < eps)) {
		all(abs(diff(x)-1) < eps)
	} else {
		FALSE
	}
}

# shorthand for frame[columnname/id,drop=FALSE]
"$.monet.frame"<-function(x,i) {
	x[i,drop=FALSE]
}

# returns a single row with one index/element with two indices
"[[.monet.frame"  <- function(x, k, j, ...) {
	x[k,j,drop=FALSE,...][[1]]
}

# overwrite S3 generic rbind() for monet.frame
# code by Anthony Damico
rbind.monet.frame <-
		function( ... ){
	
	list.of.frames <- list( ... )
	
	# confirm all objects are monet.frame objects
	if( !all( lapply( list.of.frames , class ) == 'monet.frame' ) ) stop( "all objects must have class( x ) == 'monet.frame'" )
	
	# if it's just zero or one monet.frame object, you're done1
	if ( length( list.of.frames ) < 2 ) return( list.of.frames[[ 1 ]] )
	
	# confirm all monet.frame objects have the same connection
	all.cons <- lapply( list.of.frames , attr , "conn" )
	if ( length( unique( all.cons ) ) != 1 ) stop( "all monet.frame objects must share the same connection" )
	
	
	# confirm all columns line up, sorted
	all.names <- lapply( list.of.frames , function( x ) sort( names( x ) ) )
	if ( length( unique( all.names ) ) != 1 ) stop( "all monet.frame objects must have the same column names" )
	
	# check if all columns line up, UNsorted
	all.names <- lapply( list.of.frames , function( x ) names( x ) )
	if( length( unique( all.names ) ) != 1 ){
		
		# loop through each subsequent monet.frame object
		for ( j in 2:length( list.of.frames ) ){
			
			# find the position that the frame *should* be in
			col.sort.order <- sapply( names( list.of.frames[[ 1 ]] ) , function( x ) which( x == names( list.of.frames[[ j ]] ) ) )
			
			# conduct the column sort
			list.of.frames[[ j ]] <- list.of.frames[[ j ]][ , col.sort.order ]
			
		}
		
	}
	
	# now that the columns are sorted, confirm all columns are the same type
	all.types <- lapply( list.of.frames , rTypes )
	if ( length( unique( all.types ) ) != 1 ) stop( "all monet.frame objects must have the same column types" )
	
	
	# extract each of the queries from the monet.frame objects
	all.queries <- lapply( list.of.frames , getQuery )
	
	# now just stack all tables on top of each other
	nquery <- paste( unlist( all.queries ) , collapse = " UNION ALL " )
	
	
	# NOTE:
	# the connection, the column count, column names and column types are taken from the first argument. 
	# The number of expected rows is the sum of rows of all arguments.
	# If any of the monet.frame objects has the debug flag set to TRUE, the new one will have this as well.
	
	x <- list.of.frames[[ 1 ]]
	debug <- FALSE
	nrow.hint <- 0
	
	# loop through each subsequent monet.frame object
	for ( j in 1:length( list.of.frames ) ){
		nrow.hint <- nrow.hint + nrow(list.of.frames[[j]])
		debug <- debug || .is.debug(list.of.frames[[j]])
	}
	
	# construct and return new monet.frame for rewritten query
	monet.frame.internal(attr(x,"conn"),nquery,debug,nrow.hint=nrow.hint, ncol.hint=ncol(x),cnames.hint=names(x), rtypes.hint=rTypes(x))	
}



# overwrite S3 generic merge() for monet.frame
# code by Anthony Damico
merge.monet.frame <-
	function( 
		x , y , 
		by = intersect( names(x) , names(y) ) , 
		by.x = by , by.y = by , 
		all = FALSE , all.x = all , all.y = all , 
		sort = TRUE # , 
		# suffixes = c(".x", ".y") , incomparables = NULL , ... 
	,...) {

		if ( any( grepl( "." , c( names( x ) , names( y ) ) , fixed = TRUE ) ) ) stop( "`.` not allowed in column names for merge" )
	
		# confirm all objects are monet.frame objects
		if( ( class( x ) != 'monet.frame' ) | ( class( y ) != 'monet.frame' ) ) stop( "all objects must have class( x ) == 'monet.frame'" )

		# confirm all monet.frame objects have the same connection
		all.cons <- list( attr( x , "conn" ) , attr( y , "conn" ) )
		if ( length( unique( all.cons ) ) != 1 ) stop( "x and y must share the same connection" )

		# if by.x and by.y are not the same names, merge.default keeps the columns from by.x
		# so merge.monet.frame should act the same way

		# figure out what kind of join this will be
		join.type <- 
			ifelse( all.x & all.y , "FULL" , 
			ifelse( all.x & !all.y , "LEFT" ,
			ifelse( !all.x & all.y , "RIGHT" ,
			ifelse( !all.x & !all.y , "INNER" , stop("mind blown") ) ) ) )

		
		# if it's a left join or inner join
		if ( join.type %in% c( "LEFT" , "INNER" ) ){
			# keep *all* columns from the left hand side data frame
			cols.x <- names( x )
			
			# keep only non-intersecting columns from the y data frame *and also* throw out by.y variables
			cols.y <- names( y )[ !( names( y ) %in% by.y ) ]
		} 
		# if it's a right join
		if ( join.type == "RIGHT" ){
			# flip it
			cols.y <- names( y )
			cols.x <- names( x )[ !( names( x ) %in% by.x ) ]
		} 
		
		# otherwise it's a full join
		if ( join.type == "FULL" ){
			# keep none of the by.x or by.y columns
			cols.x <- names( x )[ !( names( x ) %in% by.x ) ]		
			cols.y <- names( y )[ !( names( y ) %in% by.y ) ]		
		}
		
		
		
		# this is a warning in merge.default, but merge.monet.frame can be stricter for the time being.
		any.duplicates <- intersect( cols.x , cols.y ) 
		if ( length( any.duplicates ) > 0 ) stop( paste( "column name" , any.duplicates , "duplicated in the result" ) )
		
		# confirm merge variable vectors have the same length
		if ( length( by.x ) != length( by.y ) ) stop( 'by.x and by.x must have the same length' )
		
		
		# generate three random strings to name these tables in the temporary query
		random.x <-
			paste(
				sample(
					letters ,
					10 , 
					replace = TRUE
				) ,
				collapse = ""
			)
			
		random.y <-
			paste(
				sample(
					letters ,
					10 , 
					replace = TRUE
				) ,
				collapse = ""
			)
		
		random.full <-
			paste(
				sample(
					letters ,
					10 , 
					replace = TRUE
				) ,
				collapse = ""
			)
			
		
		# figure out both table queries and name them something new
		x.query <- paste( "(" , attr( x , "query" ) , ") as" , random.x )
		y.query <- paste( "(" , attr( y , "query" ) , ") as" , random.y )
		
		


		# if it's not a full join, just construct a standard query
		if ( join.type != 'FULL' ){
				
			# standard SELECT statement construction
			if ( join.type != "RIGHT" ){

				order.segment <-
					paste(
						paste( random.x , by.x , sep = "." ) ,
						paste( random.y , by.y , sep = "." ) ,
						sep = ", " ,
						collapse = ", "
					)

				select.segment <-
					paste(
						paste( random.x , cols.x , sep = "." , collapse = ", " ) ,
						paste( random.y , cols.y , sep = "." , collapse = ", " ) ,
						sep = ", "
					)
					
			} else {

				order.segment <-
					paste(
						paste( random.y , by.y , sep = "." ) ,
						paste( random.x , by.x , sep = "." ) ,
						sep = ", " ,
						collapse = ", "
					)

				# fancy SELECT statement construction to match the output of the merge() function
				select.segment <-
					paste(
						paste(
							paste0( rep( random.y , length( by.y ) ) , "." , by.y , " as " , by.x ) , collapse = ", "
						) ,
						paste( random.x , cols.x[ !( cols.x %in% by.x ) ] , sep = "." , collapse = ", " ) ,
						paste( random.y , cols.y[ !( cols.y %in% by.y ) ]  , sep = "." , collapse = ", " ) ,
						collapse = ", " , sep = ", "
					)
			
			}
			
			on.segment <-
				paste( 
					paste( random.x , by.x , sep = "." ) , 
					paste( random.y , by.y , sep = "." ) , 
					sep = " = " , 
					collapse = " AND "
				)
		
			join.query <-
				paste0(
					"SELECT " ,
					select.segment ,
					" FROM " , x.query , " " , join.type , " JOIN " , y.query ,
					" ON " , on.segment ,
					ifelse( sort , paste( " ORDER BY" , order.segment ) , "" ) 
				)
				
		} else {
		
			# create a UNION table of both sides and LEFT/RIGHT join to each side of that table
			union.segment <-
				paste(
					"( SELECT" ,
					paste( random.x , by.x , sep = "." , collapse = ", " ) ,
					"FROM" ,
					x.query ,
					"UNION" ,
					"SELECT" ,
					paste( random.y , by.y , sep = "." , collapse = ", " ) ,
					"FROM" ,
					y.query ,
					" ) as " ,
					random.full
				)
				
			order.segment <-
				paste(
					paste( random.full , by.x , sep = "." ) ,
					sep = ", " ,
					collapse = ", "
				)

			
			select.segment <-
				paste(
					paste( random.full , by.x , sep = "." , collapse = ", " ) ,
					paste( random.x , cols.x , sep = "." , collapse = ", " ) ,
					paste( random.y , cols.y , sep = "." , collapse = ", " ) ,
					sep = ", "
				)

			x.on.segment <-
				paste( 
					paste( random.full , by.x , sep = "." ) , 
					paste( random.x , by.x , sep = "." ) , 
					sep = " = " , 
					collapse = " AND "
				)


			y.on.segment <-
				paste( 
					paste( random.full , by.x , sep = "." ) , 
					paste( random.y , by.y , sep = "." ) , 
					sep = " = " , 
					collapse = " AND "
				)

				
			join.query <-
				paste(
					"SELECT" ,
					select.segment ,
					"FROM" , 
						union.segment ,
						"LEFT JOIN" ,
						x.query , 
						"ON" ,
						x.on.segment ,
						"LEFT JOIN" ,
						y.query ,
						"ON" ,
						y.on.segment ,
					ifelse( sort , paste( " ORDER BY" , order.segment ) , "" ) 
				)
			
		}
		
		
		list.of.frames <- list( x , y )
		debug <- FALSE
		nrow.hint <- 0
		
		# loop through each subsequent monet.frame object
		for ( j in 1:length( list.of.frames ) ){
			nrow.hint <- nrow.hint + nrow(list.of.frames[[j]])
			debug <- debug || .is.debug(list.of.frames[[j]])
		}
		
		# return the monet.frame object now connected to the new table
		monet.frame.internal(attr(x,"conn"),join.query,debug,nrow.hint=nrow.hint, ncol.hint=ncol(x),cnames.hint=names(x), rtypes.hint=rTypes(x))	
	}



str.monet.frame <- function(object, ...) {
	cat("MonetDB-backed data.frame surrogate\n")
	# i agree this is overkill, but still...
	nrows <- nrow(object)
	ncols <- ncol(object)
	rowsdesc <- "rows"
	if (nrows == 1) rowsdesc <- "row"
	colsdesc <- "columns"
	if (ncols == 1) colsdesc <- "column"
	cat(paste0(ncol(object)," ",colsdesc,", ",nrow(object)," ",rowsdesc,"\n"))
	
	cat(paste0("Query: ",getQuery(object),"\n"))	
	cat(paste0("Columns: ",paste0(names(object)," (",attr(object,"rtypes"),")",collapse=", "),"\n"))	
}


na.omit.monet.frame <- .filter.na  <- function(object,...){
	if (ncol(object) != 1) 
		stop("na.omit() only defined for one-column frames, consider using $ first")
	filter <- bquote( !is.na(.(names(object)[[1]])) )
	do.call(subset, list(object, filter))
}

na.fail.monet.frame <- function(object,...) {
	if (ncol(object) != 1) 
		stop("na.fail() only defined for one-column frames, consider using $ first")
	filter <- bquote( is.na(.(names(object)[[1]])) )
	object <- do.call(subset, list(object, filter))
	if (nrow(object) > 0)
		stop("NA/NULL values found in column '",names(object),"'. Failing as requested.")
}

# chop up frame into list of single columns. surely, that can be done more clever
as.list.monet.frame <- function(x,...) {
	cols <- list()
	for (col in seq.int(ncol(x))) {
		cols <- c(cols,x[,col,drop=FALSE])
	}
	cols
}

# adapted from summary.default
summary.monet.frame <- function (object, maxsum = 7, digits = max(3, getOption("digits") - 3), ...){

	# call data.frame summary code. here, we summarize only single columns. sumamry.data.frame will 
	# call as.list and then summary on columns, which will bring us right back here.
	
	if (ncol(object) > 1) {
		cat("Calculating summaries. This may take a while.\n")
		return(summary.data.frame(object))
	}
	col <- object
	nncol <- .filter.na(col)
	nas <- nrow(col) - nrow(nncol)
	
	doneSth <- FALSE
	value <- if (attr(col,"rtypes")[[1]] == "numeric") {
		qq <- quantile(nncol,printDots=FALSE)
		qq <- signif(c(qq[1L:3L], mean(nncol), qq[4L:5L]), digits)
		names(qq) <- c("Min.", "1st Qu.", "Median", "Mean", "3rd Qu.", 
				"Max.")
		
		if (nas > 0) qq <- c(qq, `NA's` = nas)
		qq
	}
	else {
		qq <- c(Column = names(col)[[1]],Length = nrow(col), Class = attr(col,"rtypes")[[1]], Mode = attr(col,"rtypes")[[1]])
		if (nas > 0) qq <- c(qq, `NA's` = nas)
		qq
	}
	class(value) <- c("summary.monet.frame", "table")
	value
}


print.monet.frame <- function(x, ...) {
	print(adf(x))
}

names.monet.frame <- function(x) {
	attr(x,"cnames")
}

dim.monet.frame <- function(x) {
	c(attr(x,"nrow"),attr(x,"ncol"))
}

length.monet.frame <- function(x) {
	ncol(x)
}

# http://stat.ethz.ch/R-manual/R-patched/library/base/html/subset.html
subset.monet.frame<-function(x,subset,...){
	subset<-substitute(subset)
	restr<-sqlexpr(subset,parent.frame())
	query <- getQuery(x)
	if (length(grep(" WHERE ",query,ignore.case=TRUE)) > 0) {
		nquery <- sub("WHERE (.*?) (GROUP|HAVING|ORDER|LIMIT|OFFSET|;|$)",paste0("WHERE \\1 AND ",restr," \\2"),query,ignore.case=TRUE)
	}
	else {
		nquery <- sub("(GROUP|HAVING|ORDER[ ]+BY|LIMIT|OFFSET|;|$)",paste0(" WHERE ",restr," \\1"),query,ignore.case=TRUE)
	}
	
	# construct and return new monet.frame for rewritten query
	monet.frame.internal(attr(x,"conn"),nquery,.is.debug(x),nrow.hint=NA, ncol.hint=ncol(x),cnames.hint=names(x), rtypes.hint=rTypes(x))	
}
#
#rowsum.monet.frame <- function (x, group, reorder = TRUE, na.rm = FALSE, ...) {
#	if (na.rm) x <- .filter.na(x)
#	aggregate(x,group,"sum")
#	#TODO: group has to be a column, so either a set of strings or monet.frame, where the columns will be used to group
#	
#	
#	
#} 

# basic math and comparision operators
#  ‘"+"’, ‘"-"’, ‘"*"’, ‘"/"’, ‘"^"’, ‘"%%"’, `"%/%"’ (only numeric)
#  ‘"&"’, ‘"|"’, ‘"!"’ (only boolean)
#  ‘"<"’, ‘"<="’, ‘">="’, ‘">"’  (only numeric)
#  ‘"=="’, ‘"!="’ (generic)

Ops.monet.frame <- function(e1,e2) {
	unary <- nargs() == 1L
	lclass <- nzchar(.Method[1L])
	rclass <- !unary && (nzchar(.Method[2L]))
	
	# this will be the next SELECT x thing
	nexpr <- NA
	nrow.hint <- NA
	
	left <- right <- query <- queryresult <- conn <- NA
	leftNum <- rightNum <- leftBool <- rightBool <- NA
	
	# both values are monet.frame
	if (lclass && rclass) {
		if (any(dim(e1) != dim(e2)) || ncol(e1) != 1 || ncol(e2) != 1) 
			stop(.Generic, " only defined for one-column result sets of equal length.")
		
		lquery <- query <- getQuery(e1)
		conn <- attr(e1,"conn")
		nrow.hint <- nrow(e1)
		
		isdebug <- .is.debug(e1) || .is.debug(e2)
		rquery <- getQuery(e2)
		
		left <- sub("(select )(.*?)( from.*)","(\\2)",lquery,ignore.case=TRUE)
		right <- sub("(select )(.*?)( from.*)","(\\2)",rquery,ignore.case=TRUE)
		
		leftrem <- sub("(select )(.*?)( from.*)","(\\1)X(\\3)",lquery,ignore.case=TRUE)
		rightrem <- sub("(select )(.*?)( from.*)","(\\1)X(\\3)",rquery,ignore.case=TRUE)
		
		if (leftrem != rightrem) {
			stop("left and right columns have to come from the same table with the same restrictions.")
		}
		
		# some tests for data types
				
		leftNum   <- rTypes(e1)[[1]] == "numeric"
		leftBool  <- rTypes(e1)[[1]] == "logical"
		rightNum  <- rTypes(e2)[[1]] == "numeric"
		rightBool <- rTypes(e2)[[1]] == "logical"
	}
	
	# left operand is monet.frame
	else if (lclass) {
		if (ncol(e1) != 1) 
			stop(.Generic, " only defined for one-column frames, consider using $ first")
		if (length(e2) != 1)
			stop("Only single-value constants are supported.")
		query <- getQuery(e1)
		isdebug <- .is.debug(e1)
		conn <- attr(e1,"conn")
		nrow.hint <- nrow(e1)
		
				
		left <- sub("(select )(.*?)( from.*)","(\\2)",query,ignore.case=TRUE)
	
		leftNum   <- rTypes(e1)[[1]] == "numeric"
		leftBool  <- rTypes(e1)[[1]] == "logical"
		
		right <- e2
		rightNum  <- is.numeric(e2)
		rightBool <- is.logical(e2)		
	}
	
	# right operand is monet.frame
	else {
		if (ncol(e2) != 1) 
			stop(.Generic, " only defined for one-column frames, consider using $ first")
		if (length(e1) != 1)
			stop("Only single-value constants are supported.")
		query <- getQuery(e2)
		
		right <- sub("(select )(.*?)( from.*)","(\\2)",query,ignore.case=TRUE)
		
		conn <- attr(e2,"conn")
		isdebug <- .is.debug(e2)
		nrow.hint <- nrow(e2)
		
		rightNum  <- rTypes(e2)[[1]] == "numeric"
		rightBool <- rTypes(e2)[[1]] == "logical"
		
		left <- e1
		leftNum <- is.numeric(e1)
		leftBool <- is.logical(e1)
	}
	
	rtypes.hint <- c("numeric")
	
	leftNumBool <- leftBool || leftNum
	rightNumBool <- rightBool || rightNum
	
		
	# mapping of R operators to DB operators...booring		
	if (.Generic %in% c("+", "-", "*", "/","<",">","<=",">=")) {
		if (!leftNumBool || !rightNumBool)
			stop(.Generic, " only supported for numeric or logical arguments")
		nexpr <- paste0(left,.Generic,right)
	}
	if (.Generic == "^") {
		if (!leftNumBool || !rightNumBool)
			stop(.Generic, " only supported for numeric or logical arguments")
		nexpr <- paste0("POWER(",left,",",right,")")
	}
	if (.Generic == "%%") {
		if (!leftNumBool || !rightNumBool)
			stop(.Generic, " only supported for numeric or logical arguments")
		nexpr <- paste0(left,"%",right)
	}
	
	if (.Generic == "%/%") {
		if (!leftNumBool || !rightNumBool)
			stop(.Generic, " only supported for numeric or logical arguments")
		nexpr <- paste0(left,"%CAST(",right," AS BIGINT)")
	}
	
	if (.Generic == "!") {
		if (!leftBool)
			stop(.Generic, " only supported for logical arguments")
		nexpr <- paste0("NOT(",left,")")
		rtypes.hint <- c("logical")
	}
	
	if (.Generic == "&") {
		if (!leftBool || !rightBool)
			stop(.Generic, " only supported for logical arguments")
		nexpr <- paste0(left," AND ",right)
		rtypes.hint <- c("logical")
	}
	
	if (.Generic == "|") {
		if (!leftBool || !rightBool)
			stop(.Generic, " only supported for logical arguments")
		nexpr <- paste0(left," OR ",right)
		rtypes.hint <- c("logical")
	}
	
	if (.Generic == "==") {
		nexpr <- paste0(left,"=",right)
		rtypes.hint <- c("logical")
	}
	
	if (.Generic == "!=") {
		nexpr <- paste0("NOT(",left,"=",right,")")
		rtypes.hint <- c("logical")
	}
		
	if (is.na(nexpr)) 
		stop(.Generic, " not supported (yet). Sorry.")
	
	# replace the thing between SELECT and WHERE with the new value and return new monet.frame
	nquery <- sub("select (.*?) from",paste0("SELECT ",nexpr," FROM"),query,ignore.case=TRUE)
			
	# construct and return new monet.frame for rewritten query
	cnames.hint <- c(paste0(.Generic,"_result"))

	monet.frame.internal(conn,nquery,isdebug,nrow.hint=nrow.hint,ncol.hint=1,cnames.hint=cnames.hint, rtypes.hint=rtypes.hint)	
}

# works: min/max/sum/range/prod
# TODO: implement  ‘all’, ‘any’, ‘prod’ (product)
Summary.monet.frame <- function(x,na.rm=FALSE,...) {
	if (na.rm) x <- .filter.na(x)
	adf(.col.func(x,.Generic,aggregate=TRUE))[[1,1]]
}

mean.monet.frame <- avg.monet.frame <- function(x,...) {
	adf(.col.func(x,"avg",aggregate=TRUE))[[1,1]]
}

.col.func <- function(x,func,extraarg="",aggregate=FALSE,rename=NA,num=TRUE){
	if (ncol(x) != 1) 
		stop(func, " only defined for one-column frames, consider using $ first.")
	
	colNum <- attr(x,"rtypes")[[1]] %in% c("numeric","logical")
	if (num && !colNum)
		stop(names(x), " is not a numerical or logical column.")
	
	query <- getQuery(x)
	col <- sub("(select )(.*?)( from.*)","\\2",query,ignore.case=TRUE)
		
	conn <- attr(x,"conn")
	nexpr <- NA
	
	if (func %in% c("min", "max", "sum","avg","abs","sign","sqrt","floor","ceiling","exp","log","cos","sin","tan","acos","asin","atan","cosh","sinh","tanh","stddev_pop","stddev","prod","distinct")) {
		nexpr <- paste0(toupper(func),"(",col,")")
	}
	if (func == "range") {
		return(c(.col.func(x,"min",aggregate=TRUE),.col.func(x,"max",aggregate=TRUE)))
	}
	
	if (func == "round") {
		nexpr <- paste0("ROUND(",col,",",extraarg,")")
	}
	if (func == "signif") {
		# in SQL, ROUND(123,-1) will zero 1 char from the rear (120), 
		# in R, signif(123,1) will start from the front (100)
		# so, let's adapt
		nexpr <- paste0("ROUND(",col,",-1*LENGTH(",col,")+",extraarg,")")
	}
	
	if (func == "cast") {
		nexpr <- paste0("CAST(",col," as ",extraarg,")")
	}
		
	if (is.na(nexpr)) 
		stop(func, " not supported (yet). Sorry.")
	
	if (!is.na(rename)) 
		nexpr <- paste0(nexpr," AS ",rename)
	
	# replace the thing between SELECT and WHERE with the new value and return new monet.frame
	nquery <- sub("select (.*?) from",paste0("SELECT ",nexpr," FROM"),query,ignore.case=TRUE)
			
	# construct and return new monet.frame for rewritten query
	cnames.hint <- c(paste0(func,"_result"))
	rtypes.hint <- c("numeric")
	
	nrow.hint <- NA
	if (aggregate) nrow.hint <- 1
	else nrow.hint <- nrow(x)
	
	monet.frame.internal(conn,nquery,.is.debug(x),ncol.hint=1,nrow.hint=nrow.hint,cnames.hint=cnames.hint,rtypes.hint=rtypes.hint)
}

sd.monet.frame <- function(x, na.rm = FALSE) {
	if (ncol(x) != 1) 
		stop("sd() only defined for one-column frames, consider using $ first.")
	if (na.rm) x <- .filter.na(x) 
	if (.hasColFunc(attr(x,"conn"),"stddev_pop"))
		r <- .col.func(x,"stddev_pop",aggregate=TRUE)
	else 
		r <- .col.func(x,"stddev",aggregate=TRUE)
	adf(r)[[1,1]]
}

var.monet.frame <- function (x, y = NULL, na.rm = FALSE, use) {
	if (ncol(x) != 1) 
		stop("var() only defined for one-column frames, consider using $ first.")
	if (!missing(use)) stop("use parameter not supported on var() for monet.frame objects")
	if (!missing(y)) stop("y parameter not supported on var() for monet.frame objects")
	if (na.rm) x <- .filter.na(x) 
	mean((x-mean(x))^2)
}

is.vector.monet.frame <- function (x, mode = "any") { 
	if (mode != "any") stop("Type checking not yet supported in is.vector()")
	return(ncol(x) == 1)
}

range.monet.frame <- function (...,na.rm=FALSE) {
	nargs = length(list(...))
	if (nargs != 1) stop("Need a parameter of type monet.frame")
	x <- list(...)[[1]]
	if (ncol(x) != 1) 
		stop("range() only defined for one-column frames, consider using $ first.")
	c(min(x,na.rm),max(x,na.rm))
}


# whoa, this is a beast. but it works, so all is well...
tabulate.default <- function (bin, nbins = max(1L, bin, na.rm = TRUE)) base::tabulate (bin, nbins) 
tabulate <- function (bin, nbins = max(1L, bin, na.rm = TRUE)) UseMethod("tabulate")
tabulate.monet.frame <- function (bin, nbins = max(bin)) {
	if (ncol(bin) != 1) 
		stop("tabulate() only defined for one-column frames, consider using $ first.")
	
	isNum <- rTypes(bin)[[1]] %in% c("numeric")
	if (!isNum)
		stop("tabulate() is only defined for numeric columns.")
	if (nbins > .Machine$integer.max) 
		stop("attempt to make a table with >= 2^31 elements")
	
	nbins <- as.integer(nbins)
	if (is.na(nbins)) 
		stop("invalid value of 'nbins'")
	
	# TODO: be more specific in typing, so we can check for int/double cols
	#if (!grepl("INT",dbTypes(bin)[[1]]))
	bin <- .col.func(bin,"cast","integer",FALSE,"t1",FALSE)
		
	nquery <- paste0("SELECT t1,COUNT(t1) AS ct FROM (",getQuery(bin),") AS t WHERE t1 > 0 GROUP BY t1 ORDER BY t1 LIMIT ",nbins,";");
	if (.is.debug(bin))
		cat(paste0("EX: '",nquery,"'\n",sep=""))	

	counts <- dbGetQuery(attr(bin,"conn"),nquery)	
	indices <- data.frame(t1=seq(1,nbins))
	d <- merge(indices,counts,all.x=T,by=c("t1"))$ct
	d[is.na(d)] <- 0
	return(d)
}

unique.monet.frame <- function (x, incomparables = FALSE, fromLast = FALSE, ...) {
	if (ncol(x) != 1) 
		stop("unique() only defined for one-column frames, consider using $ first.")
	as.vector(.col.func(x,"distinct",num=FALSE,aggregate=TRUE))
}

# overwrite non-generic functions sd and var
sd.default <- function(x, na.rm = FALSE) stats::sd(x,na.rm)
sd <- function(x, na.rm = FALSE) UseMethod("sd")

var.default <- function(x, y = NULL, na.rm = FALSE, use) stats::var(x, y, na.rm, use)
var <- function (x, y = NULL, na.rm = FALSE, use) UseMethod("var")

sample.default <- function (x, size, replace = FALSE, prob = NULL) base::sample(x, size, replace, prob)
sample <- function (x, size, replace = FALSE, prob = NULL) UseMethod("sample")

sample.monet.frame <- function (x, size, replace = FALSE, prob = NULL){
	if (replace) stop("replace=TRUE not supported on sample() for monet.frame objects")
	if (!missing(prob)) stop("prob parameter not supported on sample() for  monet.frame objects")
	if (!is.numeric(size) && length(size) != 1) stop("size parameter needs to be a single constant integer value")
	
	query <- nquery <- getQuery(x)
	# remove old limit/offset
	nquery <- gsub("limit[ ]+\\d+|offset[ ]+\\d+","",nquery,ignore.case=TRUE)
	# add sampling
	nquery <- sub(";? *$",paste0(" SAMPLE ",size),nquery,ignore.case=TRUE)
	# construct new object, only to immediately convert it to a data frame and return
	as.data.frame(monet.frame.internal(attr(x,"conn"),nquery,.is.debug(x),nrow.hint=size,ncol.hint=ncol(x), cnames.hint=names(x), rtypes.hint=rTypes(x)))
}


aggregatef <- function(formula, data, FUN, ..., subset, na.action = na.omit){
	if ( missing(formula) || !inherits(formula, "formula") ) stop("'formula' missing or incorrect")
	
	if (length(formula) != 3L) stop("'formula' must have both left and right hand sides")
	
	# extract both sides of the formula
	rhs <- unlist(strsplit(deparse(formula[[3L]]), " *[:+] *"))
	lhs <- unlist(strsplit(deparse(formula[[2L]]), " *[:+] *"))
	
	# if both are dots, it's an error
	if ( identical( rhs , "." ) & identical( lhs , "." ) ) stop( "both sides cannot be dots ya dot" )
	
	# if either side has a length of zero, that's a no-go
	if ( length( rhs ) == 0 | length( lhs ) == 0 ) stop( "gimme at least one column to aggregate on, and at least one more to aggregate by." )
	
	# if one side is a dot, it contains all the columns _not_ on the other side
	if ( identical( rhs , "." ) ) rhs <- names( data )[ !( names( data ) %in% lhs ) ]
	if ( identical( lhs , "." ) ) lhs <- names( data )[ !( names( data ) %in% rhs ) ]
	
	# and at this point, if not all of those columns are in the monet.frame, it's a no-go
	if ( !all( rhs %in% names( data ) ) ) stop( rhs[ !( rhs %in% names( data ) ) ] , " not in the monet.frame" )
	if ( !all( lhs %in% names( data ) ) ) stop( lhs[ !( lhs %in% names( data ) ) ] , " not in the monet.frame" )
	
	projection <- c(lhs,rhs)
	by <- as.list(rhs)
	fname <- tolower(substitute(FUN))
	
	aggregate.monet.frame(data[,projection,drop=FALSE],by,fname,...,simplify=FALSE)
}


aggregate.monet.frame <- function(x, by, FUN, ..., simplify = TRUE) {
	if (!is.character(FUN)) FUN <- tolower(substitute(FUN)) 
	else fname = tolower(FUN)

	if (fname == "mean") fname <- "avg"
	if (fname == "sd" ) fname <- "stddev_pop"
	
	if (!(fname %in% c("min","max","avg","sum","count","median","stddev")))
		stop(fname," not supported for aggregate(). Sorry.")
	
	if (length(by) ==0)
		stop("I need at least one column to aggregate on (by=...).")
	
	if (simplify)
		warning("simplify=TRUE is not supported. Overriding to FALSE.")
	
	if (!all(by %in% names(x)))
		stop(paste0("Invalid aggregation column '",paste(by,collapse=", "),"'. Column names have to be in set {",paste(names(x),collapse=", "),"}.",sep=""))			
	
	aggrcols <- names(x)[!(names(x) %in% by)]
	aggrtypes <- rTypes(x)[!(names(x) %in% by)]
	
	if (length(aggrcols) ==0)
		stop("I need at least one column to aggregate.")
	
	if (!(all(aggrtypes %in% c("numeric","logical"))) && fname != "count")
		stop("Aggregated columns have all to be numeric or logical.",)
	
	grouping <- paste0(paste0(by,"",collapse=", "))
	projection <- paste0(grouping,", ",paste0(toupper(fname),"(",aggrcols,") AS ",fname,"_",aggrcols,collapse=", "))
	
	cnames.hint <- c(paste(by),paste0(fname,"_",aggrcols))
	ncol.hint <- length(cnames.hint)
	
	rtypes.hint <- c(rTypes(x)[match(by,names(x))],aggrtypes)
	
	# part 0: remove grouping that was there before?
	# TODO (?)
	
	# part 1: project
	nquery <- sub("SELECT.+FROM",paste0("SELECT ",projection," FROM"),getQuery(x),ignore.case=TRUE)
	# part2: group by, directly after where, before having/orderby/limit/offset
	nquery <- gsub("(SELECT.*?)(HAVING|ORDER[ ]+BY|LIMIT|OFFSET|;|$)",paste0("\\1 GROUP BY ",grouping," \\2"),nquery,ignore.case=TRUE)
	
	monet.frame.internal(attr(x,"conn"),nquery,.is.debug(x),nrow.hint=NA, ncol.hint=ncol.hint,cnames.hint=cnames.hint,rtypes.hint=rtypes.hint)	
}



	
head.monet.frame <- function (x, n = 6L, ...) adf(x[1:min(nrow(x),n),])

tail.monet.frame <- function (x, n = 6L, ...) adf(x[max(nrow(x)-n+1,1):nrow(x),])

sort.monet.frame <- function (x, decreasing = FALSE, ...) {
	if (ncol(x) != 1) 
		stop("sort() only defined for one-column frames, consider using $ first.")
	# TODO: implement ORDER BY. remove previous if required.
	
	# sort by given column, either add ORDER BY x [DESC] at end of query or before LIMIT/OFFSET
	query <- getQuery(x)
	conn <- attr(x,"conn")
		
	# remove any old ORDER BY
	nquery <- sub("(SELECT .*? FROM .*?) (ORDER[ ]+BY[ ]+.*?) (LIMIT|OFFSET|;)(.*)","\\1 \\3 \\4",query,ignore.case=TRUE)
	
	# construct new
	orderby <- paste0("ORDER BY ",names(x)[[1]]) # TODO: make.db.names?
	if (decreasing) orderby <- paste0(orderby," DESC")
	
	nquery <- sub("SELECT (.*)(LIMIT|OFFSET|;|$)",paste0("SELECT \\1 ",orderby," \\2"),nquery,ignore.case=TRUE)	
	monet.frame.internal(conn,nquery,.is.debug(x),nrow.hint=nrow(x),ncol.hint=ncol(x),cnames.hint=names(x),rtypes.hint=rTypes(x))
}

quantile.monet.frame <-  function(x, probs = seq(0, 1, 0.25), na.rm = FALSE,
		names = TRUE, type = 7, printDots=FALSE, ...) {
	if (ncol(x) != 1) 
		stop("quantile() only defined for one-column frames, consider using $ first.")
	isNum <- attr(x,"rtypes")[[1]] == "numeric"
	if (!isNum)
		stop("quantile() is only defined for numeric columns.")
	if (na.rm) x <- .filter.na(x)
	n <- nrow(x)
	ret <- c()
	for (i in 1:length(probs)) {
		if (printDots) cat(".")
		index <- ceiling(probs[i]*n)+1
		if (index > n) index <- n
		# TODO: if prob = 0.5 use median()?
		y <- sort(x)[index,1,drop=FALSE]
		ret <- c(ret,as.vector(y)[[1]])
	}
	if (names) names(ret) <- paste0(as.integer(probs*100),"%")
	ret
}


median.monet.frame <- function (x, na.rm = FALSE) {
	# TODO: use median() here
	quantile(x,0.5,na.rm=na.rm,names=FALSE)[[1]]	
}


# TODO: implement remaining operations: expm1, log1p, *gamma, cum*
# Or just fallback to local calculation?
Math.monet.frame <- function(x,digits=0,...) {
	# yeah, baby...
	if (.Generic == "acosh") {
		return(log(x + sqrt(x^2-1)))
	}
	if (.Generic == "asinh") {
		return(log(x + sqrt(x^2+1)))
	}
	if (.Generic == "atanh") {
		return(0.5*log((1+x)/(1-x)))
	}
	if (.Generic == "round") {
		return(.col.func(x,"round",digits))	
	}
	if (.Generic == "trunc") {
		return(.col.func(x,"round",0))	
	}
	if (.Generic == "signif") {
		return(.col.func(x,"signif",digits))	
	}
	return(.col.func(x,.Generic))
}

# 'borrowed' from sqlsurvey, translates a subset() argument to sqlish

sqlexpr<-function(expr,env=emptyenv()){
	nms<-new.env(parent=emptyenv())
	assign("%in%"," IN ", nms)
	assign("&", " AND ", nms)
	assign("=="," = ",nms)
	assign("|"," OR ", nms)
	assign("!"," NOT ",nms)
	assign("I","",nms)
	assign("~","",nms)
	assign("(","",nms)
	out <-textConnection("str","w",local=TRUE)
	inorder<-function(e){
		if(length(e) ==1) {
			nm <- deparse(e)
			if (is.character(e))
				cat("'",e,"'",file=out,sep="")
			else if(exists(nm, env)) {
				val <- get(nm,env)
				if (is.numeric(val))
					cat(val, file=out)
				else if (is.character(val))
					cat("'",val,"'",file=out,sep="")
				else 
					cat(e, file=out)
			}
			else {
				cat(e, file=out)
			}
		} else if (e[[1]]==quote(is.na)){
			cat("(",file=out)
			inorder(e[[2]])
			cat(") IS NULL", file=out)
		} else if (length(e)==2){
			nm<-deparse(e[[1]])
			if (exists(nm, nms)) nm<-get(nm,nms)
			cat(nm, file=out)
			cat("(", file=out)
			inorder(e[[2]])
			cat(")", file=out)
		} else if (deparse(e[[1]])=="c"){
			cat("(", file=out)
			for(i in seq_len(length(e[-1]))) {
				if(i>1) cat(",", file=out)
				inorder(e[[i+1]])
			}
			cat(")", file=out)
		} else if (deparse(e[[1]])==":"){
			cat("(",file=out)
			cat(paste(eval(e),collapse=","),file=out)
			cat(")",file=out)
		} else{
			cat("(",file=out)
			inorder(e[[2]])
			nm<-deparse(e[[1]])
			if (exists(nm,nms)) nm<-get(nm,nms)
			cat(nm,file=out)
			inorder(e[[3]])
			cat(")",file=out)
		}
		
	}
	inorder(expr)
	close(out)
	paste("(",str,")")
	
}

getQuery <- function(x) {
	attr(x,"query")
}

rTypes <- function(x) {
	attr(x,"rtypes")
}

`[<-.monet.frame` <- `dim<-.monet.frame` <- `dimnames<-.monet.frame` <- `names<-.monet.frame` <- function(x, j, k, ..., value) {
	stop("write operators not (yet) supported for monet.frame")
}
