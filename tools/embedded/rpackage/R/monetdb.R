# we need this to find our MAL scripts and DLLs on Windows

monetdb_embedded_env <- new.env(parent=emptyenv())
monetdb_embedded_env$is_started <- FALSE
monetdb_embedded_env$started_dir <- ""

libfilename <- "libmonetdb5"

.onLoad <- function(libname, pkgname){
	library.dynam(libfilename, pkgname, lib.loc=libname, now=T, local=F)
}

classname <- "monetdb_embedded_connection"

monetdb_embedded_startup <- function(dir=tempdir(), quiet=TRUE, sequential=TRUE) {
	quiet <- as.logical(quiet)
	dir <- as.character(dir)
	if (length(dir) != 1) {
		stop("Need a single directory name as parameter.")
	}
	if (!dir.exists(dir) && !dir.create(dir, recursive=T)) {
		stop("Cannot create ", dir)
	}
	if (file.access(dir, mode=2) < 0) {
		stop("Cannot write to ", dir)
	}
	dir <- normalizePath(dir, mustWork=T)
	if (!monetdb_embedded_env$is_started) {
		res <- .Call("monetdb_startup_R", dir, quiet, 
			getOption('monetdb.squential', sequential), PACKAGE=libfilename)
	} else {
		if (dir != monetdb_embedded_env$started_dir) {
			stop("MonetDBLite cannot change database directories (already started in ", monetdb_embedded_env$started_dir, ", shutdown first).")
		}
		return(invisible(TRUE))
	}
	if (is.character(res)) {
		stop("Failed to initialize embedded MonetDB ", res)
	}
	monetdb_embedded_env$is_started <- TRUE
	monetdb_embedded_env$started_dir <- dir
	invisible(TRUE)
}

monetdb_embedded_query <- function(conn, query, execute=TRUE, resultconvert=TRUE) {
	query <- as.character(query)
	if (length(query) != 1) {
		stop("Need a single query as parameter.")
	}
	execute <- as.logical(execute)
	if (length(execute) != 1) {
		stop("Need a single execute flag as parameter.")
	}
	resultconvert <- as.logical(resultconvert)
	if (length(resultconvert) != 1) {
		stop("Need a single resultconvert flag as parameter.")
	}
	if (!inherits(conn, classname)) {
		stop("Invalid connection")
	}
	# make sure the query is terminated
	query <- paste(query, "\n;", sep="")
	res <- .Call("monetdb_query_R", conn, query, execute, resultconvert, PACKAGE=libfilename)

	resp <- list()
	if (is.character(res)) { # error
		resp$type <- "!" # MSG_MESSAGE
		resp$message <- res
	}
	if (is.logical(res)) { # no result set, but successful
		resp$type <- 2 # Q_UPDATE
	}
	if (is.list(res)) {
		resp$type <- 1 # Q_TABLE
		attr(res, "row.names") <- c(NA_integer_, length(res[[1]]))
  		class(res) <- "data.frame"
		names(res) <- gsub("\\", "", names(res), fixed=T)
		resp$tuples <- res
	}
	resp
}

monetdb_embedded_append <- function(conn, table, tdata, schema="sys") {
	table <- as.character(table)
	table <- gsub("(^\"|\"$)", "", table)
	
	if (length(table) != 1) {
		stop("Need a single table name as parameter.")
	}
	schema <- as.character(schema)
	if (length(schema) != 1) {
		stop("Need a single schema name as parameter.")
	}
	if (!is.data.frame(tdata)) {
		stop("Need a data frame as tdata parameter.")
	}
	if (!inherits(conn, classname)) {
		stop("Invalid connection")
	}
	.Call("monetdb_append_R", conn, schema, table, tdata, PACKAGE=libfilename)
}


monetdb_embedded_connect <- function() {
	if (!monetdb_embedded_env$is_started) {
		stop("Call monetdb_embedded_startup() first")
	}
	conn <- .Call("monetdb_connect_R", PACKAGE=libfilename)
	class(conn) <- classname
	return(conn)
}

monetdb_embedded_disconnect <- function(conn) {
	if (!inherits(conn, classname)) {
		stop("Invalid connection")
	}
	.Call("monetdb_disconnect_R", conn,  PACKAGE=libfilename)
	invisible(TRUE)
}

monetdb_embedded_shutdown <- function() {
	.Call("monetdb_shutdown_R", PACKAGE=libfilename)
	monetdb_embedded_env$is_started <- FALSE
	monetdb_embedded_env$started_dir <- ""
	invisible(TRUE)
}

