monetdb_startup <- function(dir=tempdir()) {
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
	invisible(.Call("monetdb_startup_R", dir , PACKAGE="MonetDB"))
}

monetdb_query <- function(query) {
	query <- as.character(query)
	if (length(query) != 1) {
		stop("Need a single query as parameter.")
	}
	# make sure the query is terminated
	query <- paste(query, "\n;", sep="")
	res <- .Call("monetdb_query_R", query, PACKAGE="MonetDB")
	if (is.null(res)) {
		return(invisible(FALSE))
	}
	else {
		return(as.data.frame(res, stringsAsFactors=F))
	}
}
