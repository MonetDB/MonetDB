monetdb_startup <- function(dir=tempdir()) {
	invisible(.Call("monetdb_startup_R", as.character(dir), PACKAGE="MonetDB"))
}

monetdb_query <- function(query) {
	res <- .Call("monetdb_query_R", as.character(query), PACKAGE="MonetDB")
	if (is.null(res)) {
		return(invisible(FALSE))
	}
	else {
		return(res)
	}
}