# we need this to find our MAL scripts and DLLs on Windows
installdir <- ""

.onLoad <- function(libname, pkgname){
	installdir <<- file.path(libname, pkgname, "libs")
	library.dynam("libmonetdb5", pkgname, lib.loc=libname, now=T, local=F)
}

monetdb_embedded_startup <- function(dir=tempdir(), quiet=TRUE) {
	dir <- normalizePath(as.character(dir))
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
	res <- .Call("monetdb_startup_R", installdir, dir, quiet, PACKAGE="libmonetdb5")
	if (is.character(res)) {
		stop("Failed to initialize embedded MonetDB ", res)
	}
	if (res == FALSE) {
		warning("monetdb_embedded_startup() was already called. Ignoring this invocation.")
	}
	invisible(TRUE)
}

monetdb_embedded_query <- function(query, notreally=FALSE) {
	query <- as.character(query)
	if (length(query) != 1) {
		stop("Need a single query as parameter.")
	}
	notreally <- as.logical(notreally)
	if (length(notreally) != 1) {
		stop("Need a single noreally flag as parameter.")
	}
	# make sure the query is terminated
	query <- paste(query, "\n;", sep="")
	res <- .Call("monetdb_query_R", query, notreally, PACKAGE="libmonetdb5")

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

monetdb_embedded_append <- function(table, tdata, schema="sys") {
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

	.Call("monetdb_append_R", schema, table, tdata, PACKAGE="libmonetdb5")
}
