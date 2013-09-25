require(DBI)
require(digest)

C_LIBRARY <- "MonetDB.R"

.onLoad <- function(lib, pkg) {
	library.dynam( C_LIBRARY, pkg, lib )
	.Call("mapiInit",PACKAGE=C_LIBRARY)
}

# TODO: make these values configurable in the call to dbConnect
DEBUG_IO      <- FALSE
DEBUG_QUERY   <- FALSE



# Make S4 aware of S3 classes
setOldClass(c("sockconn","connection","monetdb_mapi_conn"))

### MonetDBDriver
setClass("MonetDBDriver", representation("DBIDriver"))

# allow instantiation of this driver with MonetDB to allow existing programs to work
MonetR <- MonetDB <- MonetDBR <- MonetDB.R <- function() {
	new("MonetDBDriver")
}

setMethod("dbGetInfo", "MonetDBDriver", def=function(dbObj, ...)
			list(name="MonetDBDriver", 
					driver.version="0.8.0",
					DBI.version="0.2-5",
					client.version=NA,
					max.connections=NA)
)

# shorthand for connecting to the DB, very handy, e.g. dbListTables(mc("acs"))
mc <- function(dbname="demo", user="monetdb", password="monetdb", host="localhost",port=50000, timeout=86400, wait=FALSE,...) {
	dbConnect(MonetDB.R(),dbname,user,password,host,port,timeout,wait,...)
}

setMethod("dbConnect", "MonetDBDriver", def=function(drv,dbname="demo", user="monetdb", password="monetdb", host="localhost",port=50000, timeout=86400, wait=FALSE,...,url="") {
			if (substring(url,1,10) == "monetdb://") {
				dbname <- url
			}
			if (substring(dbname,1,10) == "monetdb://") {
				#warning("MonetDB.R: Using 'monetdb://...' URIs in dbConnect() is deprecated. Please switch to dbname, host, port named arguments.")
				rest <- substring(dbname,11,nchar(dbname))
				# split at /, so we get the dbname
				slashsplit <- strsplit(rest,"/",fixed=TRUE)
				hostport <- slashsplit[[1]][1]
				dbname <- slashsplit[[1]][2]
				
				# TODO: handle IPv6 IPs, they contain : chars, later
				if (length(grep(":",hostport,fixed=TRUE)) == 1) {
					hostportsplit <- strsplit(hostport,":",fixed=TRUE)
					host <- hostportsplit[[1]][1]
					port <- hostportsplit[[1]][2]
				} else {
					host <- hostport
				}
			}
			
			if (DEBUG_QUERY) cat(paste0("II: Connecting to MonetDB on host ",host," at port ",port, " to DB ", dbname, " with user ", user," and a non-printed password, timeout is ",timeout," seconds.\n"))
			socket <- FALSE
			if (wait) {
				repeat {
					continue <- FALSE
					tryCatch ({
								# open socket with 5-sec timeout so we can check whether everything works
								#socket <- socket <<- socketConnection(host = host, port = port, 
								#	blocking = TRUE, open="r+b",timeout = 5 )
								
								# this goes to src/mapi.c
								socket <- socket <<- .Call("mapiConnect",host,port,5,PACKAGE=C_LIBRARY)
								# authenticate
								.monetAuthenticate(socket,dbname,user,password)
								# test the connection to make sure it works before
								.mapiWrite(socket,"sSELECT 42;"); .mapiRead(socket)
								#close(socket)
								.Call("mapiDisconnect",socket,PACKAGE=C_LIBRARY)
								break
							}, error = function(e) {
								if ("connection" %in% class(socket)) {
									.Call("mapiDisconnect",socket,PACKAGE=C_LIBRARY)
								}
								cat(paste0("Server not ready(",e$message,"), retrying (ESC or CTRL+C to abort)\n"))
								Sys.sleep(1)
								continue <<- TRUE
							})
				}
			}
			
			# make new socket with user-specified timeout
			#socket <- socket <<- socketConnection(host = host, port = port, 
			#	blocking = TRUE, open="r+b",timeout = timeout) 
			socket <- socket <<- .Call("mapiConnect",host,port,timeout,PACKAGE=C_LIBRARY)
			.monetAuthenticate(socket,dbname,user,password)
			connenv <- new.env(parent=emptyenv())
			connenv$lock <- 0
			connenv$deferred <- list()
			connenv$exception <- list()
			
			return(new("MonetDBConnection",socket=socket,connenv=connenv))
		},
		valueClass="MonetDBConnection")


### MonetDBConnection, #monetdb_mapi_conn
setClass("MonetDBConnection", representation("DBIConnection",socket="externalptr",connenv="environment",fetchSize="integer"))

setMethod("dbDisconnect", "MonetDBConnection", def=function(conn, ...) {
			.Call("mapiDisconnect",conn@socket,PACKAGE=C_LIBRARY)
			TRUE
		})

setMethod("dbListTables", "MonetDBConnection", def=function(conn, ...) {
			df <- dbGetQuery(conn,"select name from sys.tables")	
			df$name
		})

setMethod("dbListFields", "MonetDBConnection", def=function(conn, name, ...) {
			if (!dbExistsTable(conn,name))
				stop(paste0("Unknown table: ",name));
			df <- dbGetQuery(conn,paste0("select columns.name as name from columns join tables on columns.table_id=tables.id where tables.name='",name,"';"))	
			df$name
		})

setMethod("dbExistsTable", "MonetDBConnection", def=function(conn, name, ...) {
			name %in% dbListTables(conn)
		})


setMethod("dbGetException", "MonetDBConnection", def=function(conn, ...) {
			conn@connenv$exception
		})


setMethod("dbReadTable", "MonetDBConnection", def=function(conn, name, ...) {
			if (!dbExistsTable(conn,name))
				stop(paste0("Unknown table: ",name));
			dbGetQuery(conn, paste0("SELECT * FROM ",name))
		})

setMethod("dbGetQuery", signature(conn="MonetDBConnection", statement="character"),  def=function(conn, statement, ...) {
			res <- dbSendQuery(conn, statement, ...)
			data <- fetch(res,-1)
			dbClearResult(res)
			return(data)
		})

# This one does all the work in this class
setMethod("dbSendQuery", signature(conn="MonetDBConnection", statement="character"),  def=function(conn, statement, ..., list=NULL,async=FALSE) {
			if(!is.null(list) || length(list(...))){
				if (length(list(...))) statement <- .bindParameters(statement, list(...))
				if (!is.null(list)) statement <- .bindParameters(statement, list)
			}		
			conn@connenv$exception <- list()
			env <- NULL
			if (DEBUG_QUERY)  cat(paste("QQ: '",statement,"'\n",sep=""))
			resp <- .mapiParseResponse(.mapiRequest(conn,paste0("s",statement,";"),async=async))
			
			env <- new.env(parent=emptyenv())
			
			if (resp$type == Q_TABLE) {
				# we have to pass this as an environment to make conn object available to result for fetching
				env$success = TRUE
				env$conn <- conn
				env$data <- resp$tuples
				resp$tuples <- NULL # clean up
				env$info <- resp
				env$delivered <- 0
				env$query <- statement
			}
			if (resp$type == Q_UPDATE || resp$type == Q_CREATE || resp$type == MSG_ASYNC_REPLY) {
				env$success = TRUE
				env$conn <- conn
				env$query <- statement
				env$info <- resp
			}
			if (resp$type == MSG_MESSAGE) {
				env$success = FALSE
				env$conn <- conn
				env$query <- statement
				env$info <- resp
				env$message <- resp$message
			}
			
			if (!env$success) {
				sp <- strsplit(env$message,"!",fixed=T)[[1]]
				if (length(sp) == 3) {
					errno <- as.numeric(sp[[2]])
					errmsg <- sp[[3]]
					conn@connenv$exception <- list(errNum=errno,errMsg=errmsg)
					stop(paste0("Unable to execute statement '",statement,"'.\nServer says '",errmsg,"' [#",errno,"]."))
				}
				else {
					conn@connenv$exception <- list(errNum=-1,errMsg=env$message)
					stop(paste0("Unable to execute statement '",statement,"'.\nServer says '",env$message,"'."))
				}
			}
			
			return(new("MonetDBResult",env=env))
		})


# adapted from RMonetDB, very useful...
setMethod("dbWriteTable", "MonetDBConnection", def=function(conn, name, value, overwrite=TRUE, ...) {
			if (is.vector(value) && !is.list(value)) value <- data.frame(x=value)
			if (length(value)<1) stop("value must have at least one column")
			if (is.null(names(value))) names(value) <- paste("V",1:length(value),sep='')
			if (length(value[[1]])>0) {
				if (!is.data.frame(value)) value <- as.data.frame(value, row.names=1:length(value[[1]]))
			} else {
				if (!is.data.frame(value)) value <- as.data.frame(value)
			}
			fts <- sapply(value, dbDataType, dbObj=conn)
			
			if (dbExistsTable(conn, name)) {
				if (overwrite) dbRemoveTable(conn, name)
				else stop("Table `",name,"' already exists")
			}
			
			fdef <- paste(make.db.names(conn,names(value),allow.keywords=FALSE),fts,collapse=',')
			qname <- make.db.names(conn,name,allow.keywords=FALSE)
			ct <- paste("CREATE TABLE ",qname," (",fdef,")",sep= '')
			dbSendUpdate(conn, ct)
			
			if (length(value[[1]])) {
				inss <- paste("INSERT INTO ",qname," VALUES(", paste(rep("?",length(value)),collapse=','),")",sep='')
				
				.mapiRequest(conn, "Xauto_commit 0")
				for (j in 1:length(value[[1]])) dbSendUpdate(conn, inss, list=as.list(value[j,]))
				dbSendQuery(conn,"COMMIT")
				.mapiRequest(conn, "Xauto_commit 1")
			}
			TRUE
		})


setMethod("dbDataType", signature(dbObj="MonetDBConnection", obj = "ANY"), def = function(dbObj, obj, ...) {
			if (is.logical(obj)) "BOOLEAN"
			else if (is.integer(obj)) "INTEGER"
			else if (is.numeric(obj)) "DOUBLE PRECISION"
			else if (is.raw(obj)) "BLOB"
			
			else "VARCHAR(255)"
		}, valueClass = "character")


setMethod("dbRemoveTable", "MonetDBConnection", def=function(conn, name, ...) {
			if (dbExistsTable(conn,name)) 
				dbSendUpdate(conn, paste("DROP TABLE", name))
			return(TRUE)
			return(FALSE)
		})

# for compatibility with RMonetDB (and dbWriteTable support), we will allow parameters to this method, but will not use prepared statements internally
if (is.null(getGeneric("dbSendUpdate"))) setGeneric("dbSendUpdate", function(conn, statement,...,async=FALSE) standardGeneric("dbSendUpdate"))
setMethod("dbSendUpdate", signature(conn="MonetDBConnection", statement="character"),  def=function(conn, statement, ..., list=NULL,async=FALSE) {
			if(!is.null(list) || length(list(...))){
				if (length(list(...))) statement <- .bindParameters(statement, list(...))
				if (!is.null(list)) statement <- .bindParameters(statement, list)
			}
			res <- dbSendQuery(conn,statement,async=async)
			if (!res@env$success) {
				stop(paste(statement,"failed!\nServer says:",res@env$message))
			}
			TRUE
		})

# this can be used in finalizers to not mess up the socket
if (is.null(getGeneric("dbSendUpdateAsync"))) setGeneric("dbSendUpdateAsync", function(conn, statement, ...) standardGeneric("dbSendUpdateAsync"))
setMethod("dbSendUpdateAsync", signature(conn="MonetDBConnection", statement="character"),  def=function(conn, statement, ..., list=NULL) {
			dbSendUpdate(conn,statement,async=TRUE)
		})

.bindParameters <- function(statement,param) {
	for (i in 1:length(param)) {
		value <- param[[i]]
		valueClass <- class(value)
		if (is.na(value)) 
			statement <- sub("?","NULL",statement,fixed=TRUE)
		else if (valueClass %in% c("numeric","logical","integer"))
			statement <- sub("?",value,statement,fixed=TRUE)
		else if (valueClass == c("raw"))
			stop("raw() data is so far only supported when reading from BLOBs")
		else # TODO: escaping
			statement <- sub("?",paste("'",.mapiQuote(toString(value)),"'",sep=""),statement,fixed=TRUE)
	}
	return(statement)
}

.mapiQuote <- function(str) {
	qs <- ""
	chrs <- unlist(strsplit(str,"",fixed=TRUE))
	for (chr in chrs) {
		f <- ""
		if (chr == "\n") f <- qs <- paste0(qs,"\\","n")
		if (chr == "\t") f <- qs <- paste0(qs,"\\","t")
		if (chr == "'" ) f <- qs <- paste0(qs,"\\'")
		# TODO: check null char, but how?
		#if (c == "\0") qs <- paste(qs,"\\","0",sep="")
		if (nchar(f) == 0) qs <- paste0(qs,chr)
	}
	qs
}


### MonetDBResult
setClass("MonetDBResult", representation("DBIResult",env="environment"))

.CT_NUM <- 1L
.CT_CHR <- 2L
.CT_CHRR <- 3L
.CT_BOOL <- 4L
.CT_RAW <- 5L

monetdbRtype <- function(dbType) {
	dbType <- toupper(dbType)
	
	if (dbType %in% c("TINYINT","SMALLINT","INT","BIGINT","REAL","DOUBLE","DECIMAL","WRD")) {			
		return("numeric")
	}
	if (dbType %in% c("CHAR","VARCHAR","CLOB","STR")) {
		return("character")		
	}
	if (dbType %in% c("INTERVAL","DATE","TIME","TIMESTAMP")) {
		return("date")	
	}
	if (dbType == "BOOLEAN") {
		return("logical")			
	}
	if (dbType == "BLOB") {
		return("raw")
	}
	stop("Unknown DB type ", dbType)
}


# most of the heavy lifting here
setMethod("fetch", signature(res="MonetDBResult", n="numeric"), def=function(res, n, ...) {
			if (!res@env$success) {
				stop(paste0("Cannot fetch results from error response, error was ",res@env$info$message))
			}
			
			# okay, so we arrive here with the tuples from the first result in res@env$data as a list
			info <- res@env$info
			stopifnot(res@env$delivered <= info$rows, info$index <= info$rows)
			remaining <- info$rows - res@env$delivered
			
			
			#str(res@env$tuples)
			
			if (n < 0) {
				n <- remaining
			} else {
				n <- min(n,remaining)
			}  
			
			# prepare the result holder df with columns of the appropriate type
			df = list()
			ct <- rep(0L,info$cols)
			
			for (i in seq.int(info$cols)) {
				rtype <- monetdbRtype(info$types[i])
				if (rtype=="numeric") {			
					df[[i]] <- numeric()
					ct[i] <- .CT_NUM
				}
				if (rtype=="character") {
					df[[i]] <- character()
					ct[i] <- .CT_CHR			
				}
				if (rtype=="date") {
					df[[i]] <- character()
					ct[i] <- .CT_CHRR			
				}
				if (rtype=="logical") {
					df[[i]] <- logical()
					ct[i] <- .CT_BOOL			
				}
				if (rtype=="raw") {
					df[[i]] <- raw()
					ct[i] <- .CT_RAW
				}
				names(df)[i] <- info$names[i]
			}
			
			# we have delivered everything, return empty df (spec is not clear on this one...)
			if (n < 1) {
				return(data.frame(df))
			}
			
			# now, if our tuple cache in res@env$data does not contain n rows, we have to fetch from server until it does
			if (length(res@env$data) < n) {
				cresp <- .mapiParseResponse(.mapiRequest(res@env$conn,paste0("Xexport ",.mapiLongInt(info$id)," ", .mapiLongInt(info$index), " ", .mapiLongInt(n-length(res@env$data)))))
				stopifnot(cresp$type == Q_BLOCK && cresp$rows > 0)
				
				res@env$data <- c(res@env$data,cresp$tuples)
				info$index <- info$index + cresp$rows
			}
			
			# convert tuple string vector into matrix so we can access a single column efficiently
			# stupid MAPI, [, ] and , or \t are completely unneccessary
			
			#rawdata <- gsub(",\t", "\t", res@env$data[1:n],fixed=T)
			#rawdata <- gsub("^\\[ ", "",rawdata)
			#rawdata <- gsub("\t\\]$", "", rawdata)
			#parts <- do.call("rbind", strsplit(rawdata,"\t",fixed=TRUE,useBytes=TRUE))
			#parts[parts=="NULL"] <- NA
			
			# call to a faster C implementation for the hard and annoying task of splitting everyting into fields
			parts <- .Call("mapiSplit", res@env$data[1:n],info$cols, PACKAGE=C_LIBRARY)
			
			# convert values column by column
			for (j in seq.int(info$cols)) {	
				col <- ct[[j]]
				if (col == .CT_NUM) 
					df[[j]] <- as.numeric(parts[[j]])
				if (col == .CT_CHRR) 
					df[[j]] <- parts[[j]]
				if (col == .CT_BOOL) 
					df[[j]] <- parts[[j]]=="true"
				if (col == .CT_CHR) { 
					#strings <- parts[,j]	
					#df[[j]] <- substring(strings,2,nchar(strings)-1)
					df[[j]] <- parts[[j]]
				}
				if (col == .CT_RAW) {
					df[[j]] <- lapply(parts[[j]],charToRaw)
				}
			}
			
			# remove the already delivered tuples from the background holder or clear it altogether
			if (n+1 >= length(res@env$data)) {
				res@env$data <- character()
			}
			else {
				res@env$data <- res@env$data[seq(n+1,length(res@env$data))]
			}
			res@env$delivered <- res@env$delivered + n
			
			# this is a trick so we do not have to call data.frame(), which is expensive
			attr(df, "row.names") <- c(NA_integer_, length(df[[1]]))
			class(df) <- "data.frame"
			
			return(df)
		})


setMethod("dbClearResult", "MonetDBResult",	def = function(res, ...) {
			.mapiRequest(res@env$conn,paste0("Xclose ",res@env$info$id),async=TRUE)
			TRUE	
		},valueClass = "logical")

setMethod("dbHasCompleted", "MonetDBResult", def = function(res, ...) {
			return(res@env$delivered == res@env$info$rows)
		},valueClass = "logical")


monetTypes<-rep(c("numeric","character","character","logical","raw"), c(8, 3,4,1,1))
names(monetTypes)<-c(c("TINYINT","SMALLINT","INT","BIGINT","REAL","DOUBLE","DECIMAL","WRD"),
		c("CHAR","VARCHAR","CLOB"),
		c("INTERVAL","DATE","TIME","TIMESTAMP"),
		"BOOLEAN",
		"BLOB")


setMethod("dbColumnInfo", "MonetDBResult", def = function(res, ...) {
			return(data.frame(field.name=res@env$info$names,field.type=res@env$info$types, data.type=monetTypes[res@env$info$types]))	
		},
		valueClass = "data.frame")

setMethod("dbGetInfo", "MonetDBResult", def=function(dbObj, ...) {
			return(list(statement=dbObj@env$query,rows.affected=0,row.count=dbObj@env$info$rows,has.completed=dbHasCompleted(dbObj),is.select=TRUE))	
		}, valueClass="list")


### Private Constants & Methods

PROTOCOL_v8 <- 8
PROTOCOL_v9 <- 9
MAX_PACKET_SIZE <- 8192 # determined by fair guessing, haha

HASH_ALGOS <- c("md5", "sha1", "crc32", "sha256","sha512")

MSG_REDIRECT <- "^"
MSG_MESSAGE  <- "!"
MSG_PROMPT   <- ""
MSG_QUERY    <- "&"
MSG_SCHEMA_HEADER <- "%"
MSG_ASYNC_REPLY <- "_"


Q_TABLE       <- 1 
Q_UPDATE      <- 2 
Q_CREATE      <- 3
Q_TRANSACTION <- 4
Q_PREPARE     <- 5
Q_BLOCK       <- 6




REPLY_SIZE    <- 100 # Apparently, -1 means unlimited, but we will start with a small result set. 
# The entire set might never be fetch()'ed after all!

# .mapiRead and .mapiWrite implement MonetDB's MAPI protocol. It works as follows: 
# Ontop of the socket stream, blocks are being sent. Each block has a two-byte header. 
# MAPI protocol messages are sent as one or more blocks in both directions.
# The block header contains two pieces of information: 1) the amount of bytes in the block, 
# 2) a flag indicating whether the block is the final block of a message.

# this is a combination of read and write to synchronize access to the socket. 
# otherwise, we could have issues with finalizers

.mapiRequest <- function(conObj,msg,async=FALSE) {
	# call finalizers on disused objects. At least avoids concurrent access to socket.
	#gc()
	
	if (!identical(class(conObj)[[1]],"MonetDBConnection"))
		stop("I can only be called with a MonetDBConnection as parameter, not a socket.")
	
	# ugly effect of finalizers, sometimes, single-threaded R can get concurrent calls to this method.
	if (conObj@connenv$lock > 0) {
		if (async) {
			if (DEBUG_IO) cat(paste0("II: Attempted parallel access to socket. Deferred query '",msg,"'\n"))
			conObj@connenv$deferred <- c(conObj@connenv$deferred,msg)
			return("_")
		} else {
			stop("II: Attempted parallel access to socket. Use only dbSendUpdateAsync() from finalizers.\n")
		}
	}
	
	# add exit handler that will clean up the socket and release the lock.
	# just in case someone (Anthony!) uses ESC or CTRL-C while running some long running query.
	on.exit(.mapiCleanup(conObj),add=TRUE)
	
	# prevent other calls to .mapiRequest while we are doing something on this connection.
	conObj@connenv$lock <- 1
	
	# send payload and read response		
	#.mapiWrite(conObj@socket,msg)
	#resp <- .mapiRead(conObj@socket)
	
	if (DEBUG_IO)  cat(paste("TX: '",msg,"'\n",sep=""))	
	resp <- .Call("mapiRequest",conObj@socket,msg,PACKAGE=C_LIBRARY)
	if (DEBUG_IO) {
		dstr <- resp
		if (nchar(dstr) > 300) {
			dstr <- paste0(substring(dstr,1,200),"...",substring(dstr,nchar(dstr)-100,nchar(dstr))) 
		} 
		cat(paste0("RX: '",dstr,"'\n"))
	}
	
	# get deferred statements from deferred list and execute
	while (length(conObj@connenv$deferred) > 0) {
		# take element, execute, check response for prompt
		dmesg <- conObj@connenv$deferred[[1]]
		conObj@connenv$deferred[[1]] <- NULL
		dresp <- .mapiParseResponse(.Call("mapiRequest",conObj@socket,dmesg,PACKAGE=C_LIBRARY))
		if (dresp$type == MSG_MESSAGE) {
			conObj@connenv$lock <- 0
			warning(paste("II: Failed to execute deferred statement '",dmesg,"'. Server said: '",dresp$message,"'\n"))
		}
	}	
	# release lock
	conObj@connenv$lock <- 0
	
	return(resp)
}

.mapiCleanup <- function(conObj) {
	if (conObj@connenv$lock > 0) {
		if (DEBUG_QUERY) cat("II: Interrupted query execution.\n")
		conObj@connenv$lock <- 0
	}
}

.mapiRead <- function(con) {
	if (!identical(class(con)[[1]],"externalptr"))
		stop("I can only be called with a MonetDB connection object as parameter.")
	respstr <- .Call("mapiRead",con,PACKAGE=C_LIBRARY)
	if (DEBUG_IO) {
		dstr <- respstr
		if (nchar(dstr) > 300) {
			dstr <- paste0(substring(dstr,1,200),"...",substring(dstr,nchar(dstr)-100,nchar(dstr))) 
		} 
		cat(paste0("RX: '",dstr,"'\n"))
	}
	return(respstr)
}

.mapiWrite <- function(con,msg) {
	if (!identical(class(con)[[1]],"externalptr"))
		stop("I can only be called with a MonetDB connection object as parameter.")
	
	if (DEBUG_IO)  cat(paste("TX: '",msg,"'\n",sep=""))	
	.Call("mapiWrite",con,msg,PACKAGE=C_LIBRARY)
	return (NULL)
}



.mapiLongInt <- function(someint) {
	stopifnot(length(someint) == 1)
	formatC(someint,format="d")
}

# determines and partially parses the answer from the server in response to a query
.mapiParseResponse <- function(response) {
	#lines <- .Call("mapiSplitLines", response,PACKAGE="MonetDB.R")
	lines <- strsplit(response,"\n",fixed=TRUE,useBytes=TRUE)[[1]]
	
	typeLine <- lines[[1]]
	resKey <- substring(typeLine,1,1)
	
	if (resKey == MSG_ASYNC_REPLY) {
		return(list(type=MSG_ASYNC_REPLY))
	}
	if (resKey == MSG_MESSAGE) {
		return(list(type=MSG_MESSAGE,message=typeLine))
	}
	if (resKey == MSG_QUERY) {
		typeKey <- as.integer(substring(typeLine,2,2))
		env <- new.env(parent=emptyenv())
		
		# Query results
		if (typeKey == Q_TABLE || typeKey == Q_PREPARE) {
			header <- .mapiParseHeader(lines[1])
			if (DEBUG_QUERY) cat(paste("QQ: Query result for query ",header$id," with ",header$rows," rows and ",header$cols," cols, ",header$index," rows\n",sep=""))
			
			env$type	<- Q_TABLE
			env$id		<- header$id
			env$rows	<- header$rows
			env$cols	<- header$cols
			env$index	<- header$index
			env$tables	<- .mapiParseTableHeader(lines[2])
			env$names	<- .mapiParseTableHeader(lines[3])
			env$types	<- env$dbtypes <- toupper(.mapiParseTableHeader(lines[4]))
			env$lengths	<- .mapiParseTableHeader(lines[5])
			
			env$tuples <-lines[6:length(lines)]

			stopifnot(length(env$tuples) == header$index)
			return(env)
		}
		# Continuation of Q_TABLE without headers describing table structure
		if (typeKey == Q_BLOCK) {
			header <- .mapiParseHeader(lines[1],TRUE)
			if (DEBUG_QUERY) cat(paste("QQ: Continuation for query ",header$id," with ",header$rows," rows and ",header$cols," cols, index ",header$index,"\n",sep=""))
			
			env$type	<- Q_BLOCK
			env$id		<- header$id
			env$rows	<- header$rows
			env$cols	<- header$cols
			env$index	<- header$index
			env$tuples <- lines[2:length(lines)]
			
			stopifnot(length(env$tuples) == header$rows)
			
			return(env)
		}
		
		# Result of db-altering query
		if (typeKey == Q_UPDATE || typeKey == Q_CREATE) {
			header <- .mapiParseHeader(lines[1],TRUE)
			
			env$type	<- Q_UPDATE
			env$id		<- header$id
			
			return(env)			
		}
		
		if (typeKey == Q_TRANSACTION) {
			env$type	<- Q_UPDATE
			# TODO: actually check results of transaction...
			return(env)
		}
		
	}
}

.mapiParseHeader <- function(line, stupidInverseColsRows=FALSE) {
	tableinfo <- strsplit(line," ",fixed=TRUE,useBytes=TRUE)
	tableinfo <- tableinfo[[1]]
	
	id    <- as.numeric(tableinfo[2])
	if (!stupidInverseColsRows) {
		rows  <- as.numeric(tableinfo[3])
		cols  <- as.numeric(tableinfo[4])
	}
	else {
		rows  <- as.numeric(tableinfo[4])
		cols  <- as.numeric(tableinfo[3])
	}
	index <- as.numeric(tableinfo[5])
	
	return(list(id=id,rows=rows,cols=cols,index=index))
}

.mapiParseTableHeader <- function(line) {
	first <- strsplit(substr(line,3,nchar(line))," #",fixed=TRUE,useBytes=TRUE)
	second <- strsplit(first[[1]][1],",\t",fixed=TRUE,useBytes=TRUE)
	second[[1]]
}

# authenticates the client against the MonetDB server. This is the first thing that happens
# on each connection. The server starts by sending a challenge, to which we respond by
# hashing our login information using the server-requested hashing algorithm and its salt.

.monetAuthenticate <- function(con,dbname,user="monetdb",password="monetdb",endhashfunc="sha512") {
	endhashfunc <- tolower(endhashfunc)
	# read challenge from server, it looks like this
	# oRzY7XZr1EfNWETqU6b2:merovingian:9:RIPEMD160,SHA256,SHA1,MD5:LIT:SHA512:
	# salt:protocol:protocolversion:hashfunctions:endianness:hashrequested
	challenge <- .mapiRead(con)
	credentials <- strsplit(challenge,":",fixed=TRUE)
	
	algos <- strsplit(credentials[[1]][4],",",fixed=TRUE)
	protocolVersion <- credentials[[1]][3]
	
	if (protocolVersion != PROTOCOL_v9) {
		stop("Protocol versions != 9 NOT SUPPORTED")
	}
	
	pwhashfunc <- tolower(credentials[[1]][6])
	salt <- credentials[[1]][1]
	
	if (!(endhashfunc %in% HASH_ALGOS)) {
		stop(paste("Hash function",pwhashfunc,"is not available on server"))
	}
	
	if (!(pwhashfunc %in% HASH_ALGOS)) {
		stop(paste("Server-requested password hash function",pwhashfunc,"is not available"))
	}
	
	# We first hash the password with the server-requested hash function 
	# (SHA512 in the example above).
	# then, we hash that hash together with the server-provided salt using 
	# a hash function from the list provided by the server. 
	# By default, we use SHA512 for both.
	hashsum <- digest(paste0(digest(password,algo=pwhashfunc,serialize=FALSE),salt),algo=endhashfunc,serialize=FALSE)	
	
	# we respond to the authentication challeng with something like
	# LIT:monetdb:{SHA512}eec43c24242[...]cc33147:sql:acs
	# endianness:username:passwordhash:scenario:databasename
	authString <- paste("LIT:",user,":{",toupper(endhashfunc),"}",hashsum,":sql:",dbname,":",sep="")
	.mapiWrite(con,authString)
	authResponse <- .mapiRead(con)
	respKey <- substring(authResponse,1,1)
	
	if (respKey != MSG_PROMPT) {
		if (respKey == MSG_REDIRECT) {
			redirects <- strsplit(authResponse,"\n",fixed=TRUE)
			link <- substr(redirects[[1]][1],7,nchar(redirects[[1]][1]))
			
			redirect <- strsplit(link,"://",fixed=TRUE)
			protocol <- redirect[[1]][1]
			if (protocol == "merovingian") {
				# retry auth on same connection, we will get a new challenge
				.monetAuthenticate(con,dbname,user,password,endhashfunc)
			}
			if (protocol == "monetdb") {
				stop(paste0("Forwarding to another server (",link,") not supported."))
			}
		}
		if (respKey == MSG_MESSAGE) {
			stop(paste("Authentication error:",authResponse))
		}
	} else {
		if (DEBUG_QUERY) cat ("II: Authentication successful\n")
		# setting some server parameters...not sure if this should happen here
		.mapiWrite(con, paste0("Xreply_size ",REPLY_SIZE)); .mapiRead(con)
		.mapiWrite(con, "Xauto_commit 1"); .mapiRead(con)
	}
	
}

.hasColFunc <- function(conn,func) {
	tryCatch({
				r <- dbSendQuery(conn,paste0("SELECT ",func,"(1);"))
				TRUE
			}, error = function(e) {
				FALSE
			})
}

# copied from RMonetDB, no java-specific things in here...
# TODO: read first few rows with read.table and check types etc.

monet.read.csv <- monetdb.read.csv <- function(conn,files,tablename,nrows,header=TRUE,locked=FALSE,na.strings="",...,nrow.check=500,delim=","){
	if (length(na.strings)>1) stop("na.strings must be of length 1")
	headers<-lapply(files,read.csv,na.strings="NA",...,nrows=nrow.check)
	
	if (length(files)>1){
		nn<-sapply(headers,ncol)
		if (!all(nn==nn[1])) stop("Files have different numbers of columns")
		nms<-sapply(headers,names)
		if(!all(nms==nms[,1])) stop("Files have different variable names")
		types<-sapply(headers,function(df) sapply(df,dbDataType,dbObj=conn))
		if(!all(types==types[,1])) stop("Files have different variable types")
	} 
	
	dbWriteTable(conn, tablename, headers[[1]][FALSE,])
	
	if(header || !missing(nrows)){
		if (length(nrows)==1) nrows<-rep(nrows,length(files))
		for(i in seq_along(files)) {
			cat(files[i],thefile<-normalizePath(files[i]),"\n")
			dbSendUpdate(conn, paste("copy",format(nrows[i],scientific=FALSE),"offset 2 records into", tablename,"from",paste("'",thefile,"'",sep=""),"using delimiters ',','\\n','\"' NULL as",paste("'",na.strings[1],"'",sep=""),if(locked) "LOCKED"))
		}
	} else {
		for(i in seq_along(files)) {
			cat(files[i],thefile<-normalizePath(files[i]),"\n")
			dbSendUpdate(conn, paste0("copy into ", tablename," from ",paste("'",thefile,"'",sep="")," using delimiters '",delim,"','\\n','\"' NULL as ",paste("'",na.strings[1],"'",sep=""),if(locked) " LOCKED "))
		}
	}
	dbGetQuery(conn,paste("select count(*) from",tablename))
}

counterenv <- new.env(parent=emptyenv())
counterenv$bytes.in <-  	counterenv$bytes.out <- numeric(1)

monetdbGetTransferredBytes <- function() {
	ret <- list(bytes.in=counterenv$bytes.in,bytes.out=counterenv$bytes.out)
	counterenv$bytes.in <-  	counterenv$bytes.out <- numeric(1)
	ret
}
