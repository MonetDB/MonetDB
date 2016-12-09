# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

# R environment adaptations for MonetDB-embedded operations

# auto-install packages by intercepting library()
.library.original <- library

# configure our own library path in the dbfarm
.libPaths(.rapi.libdir)

library <- function(package, help, pos = 2, lib.loc = .libPaths(), character.only = FALSE, 
    logical.return = FALSE, warn.conflicts = TRUE, quietly = FALSE, 
    verbose = getOption("verbose")) {
	if (!character.only) 
	    package <- as.character(substitute(package))
	if (!(package %in% installed.packages()[,"Package"])) 
		install.packages(package,repos=c("http://cran.rstudio.com/"),lib=.rapi.libdir,quiet=T)
	.library.original(package,help,pos,lib.loc,character.only=T,logical.return,warn.conflicts,quietly)
}

# do not re-install existing packages if install.packages() is called
install.packages.original <- install.packages

# redirect default graphics device to PDF
options(device="pdf")
# where should plot files be written to?

# Rewire various dangerous functions, both in current scope as well as in base environment
# You could still call .Internal(quit("no", 0, T)) or .Internal(system("/", F)), 
# but we patch R structures to disallow .Internal calls to these functions in RAPIinitialize
# however, there might still be R packages who allow this. So not perfect.

rewireFunc <- function(x,value,ns) {
	ns <- asNamespace(ns)
	unlockBinding(x, ns)
    assign(x, value, envir = ns, inherits = FALSE)
    lockBinding(x, ns)
}

quit <- q <- function(...) stop("We do not want to call q(uit), it would exit MonetDB, too. You probably want to exit the R context, so I am calling stop() instead.")
rewireFunc("quit", quit, "base")
rewireFunc("q", quit, "base")

# install.packages() uses system2 to call gcc etc., so we cannot disable it
#system <- system2 <- function(...) stop("Calling external programs is sort of a no-no.")
#rewireFunc("system", system, "base")
#rewireFunc("system2", system, "base")

rm(rewireFunc)

loopback_query <- function(query) {
	dyn.load(file.path(MONETDB_LIBDIR, "monetdb5", "lib_rapi.so"))
	res <- .Call("RAPIloopback", paste0(query, "\n;"), package="lib_rapi")
	if (is.character(res)) {
		stop(res)
	}
	if (is.logical(res)) { # no result set, but successful
		return(data.frame())
	}
	if (is.list(res)) {
		attr(res, "row.names") <- c(NA_integer_, length(res[[1]]))
  		class(res) <- "data.frame"
		names(res) <- gsub("\\", "", names(res), fixed=T)
		return(res)
	}
}
