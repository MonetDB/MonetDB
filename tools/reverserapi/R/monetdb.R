.onLoad <- function(libname, pkgname){
	dyn.load(file.path(libname, pkgname, "libs", "MonetDB.so"), local=F, now=F)
}

monetdb_embedded_startup <- function(dir=tempdir(), quiet=T) {
	dir <- as.character(dir)
	quiet <- as.logical(quiet)
	if (length(dir) != 1) {
		stop("Need a single directory name as parameter.")
	}
	if (!dir.exists(dir) && !dir.create(dir, recursive=T)) {
		stop("Cannot create ", dir)
	}
	if (file.access(dir, mode=2) < 0) {
		stop("Cannot write to ", dir)
	}
	res <- .Call("monetdb_startup_R", dir, quiet)
	if (res < 0) {
		stop("Failed to initialize embedded MonetDB.")
	}
	invisible(TRUE)
}

monetdb_embedded_query <- function(query) {
	query <- as.character(query)
	if (length(query) != 1) {
		stop("Need a single query as parameter.")
	}
	# make sure the query is terminated
	query <- paste(query, "\n;", sep="")
	res <- .Call("monetdb_query_R", query)
	if (is.null(res)) {
		return(invisible(FALSE))
	}
	else {
		return(as.data.frame(res, stringsAsFactors=F))
	}
}
