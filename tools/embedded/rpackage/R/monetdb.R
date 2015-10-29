# we need this to find our MAL scripts and DLLs on Windows

monetdb_embedded_env <- new.env(parent=emptyenv())
monetdb_embedded_env$is_started <- FALSE
monetdb_embedded_env$started_dir <- ""
monetdb_embedded_env$install_dir <- ""

.onLoad <- function(libname, pkgname){
	monetdb_embedded_env$install_dir <- file.path(libname, pkgname, "libs")
	library.dynam("libmonetdb5", pkgname, lib.loc=libname, now=T, local=F)
}

classname <- "monetdb_embedded_connection"

monetdb_embedded_startup <- function(dir=tempdir(), quiet=TRUE) {
	dir <- normalizePath(as.character(dir), mustWork=FALSE)
	quiet <- as.logical(quiet)
	if (length(dir) != 1) {
		stop("Need a single directory name as parameter.")
	}
	if (!file.exists(dir) && !dir.create(dir, recursive=T)) {
		stop("Cannot create ", dir)
	}
	if (file.access(dir, mode=2) < 0) {
		stop("Cannot write to ", dir)
	}
	if (!monetdb_embedded_env$is_started) {
		res <- .Call("monetdb_startup_R", monetdb_embedded_env$install_dir, dir, quiet, PACKAGE="libmonetdb5")
	} else {
		if (dir != monetdb_embedded_env$started_dir) {
			warning("MonetDBLite cannot change database directories (already started in ", monetdb_embedded_env$started_dir, ").")
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

monetdb_embedded_query <- function(conn, query, notreally=FALSE) {
	query <- as.character(query)
	if (length(query) != 1) {
		stop("Need a single query as parameter.")
	}
	notreally <- as.logical(notreally)
	if (length(notreally) != 1) {
		stop("Need a single noreally flag as parameter.")
	}
	if (!inherits(conn, classname)) {
		stop("Need a embedded monetdb connection as parameter")
	}
	# make sure the query is terminated
	query <- paste(query, "\n;", sep="")
	res <- .Call("monetdb_query_R", conn, query, notreally, PACKAGE="libmonetdb5")

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
		stop("Need a embedded monetdb connection as parameter")
	}
	.Call("monetdb_append_R", conn, schema, table, tdata, PACKAGE="libmonetdb5")
}


monetdb_embedded_connect <- function() {
	if (!monetdb_embedded_env$is_started) {
		stop("Call monetdb_embedded_startup() first")
	}
	res <- .Call("monetdb_connect_R", PACKAGE="libmonetdb5")
	class(res) <- classname
	return(res)
}

monetdb_embedded_disconnect <- function(conn) {
	if (!inherits(conn, classname)) {
		stop("Need a embedded monetdb connection as parameter")
	}
	.Call("monetdb_disconnect_R", conn,  PACKAGE="libmonetdb5")
	return(invisible(TRUE))
}

