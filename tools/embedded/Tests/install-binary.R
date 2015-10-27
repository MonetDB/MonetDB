basedir <- Sys.getenv("TSTTRGDIR")
srcdir <- Sys.getenv("TSTSRCDIR")
if (basedir == "" || srcdir == "") {
	stop("Need TSTTRGDIR and TSTSRCDIR environment vars")
}

installdir <- file.path(basedir, "rlibdir")
if (file.exists(installdir)) file.remove(installdir, recursive=T, showWarnings=F)
dir.create(installdir)

dd <- capture.output(suppressMessages( {
	sink(file=file(tempfile(), open = "wt"), type = "message") 

	install.packages(c("MonetDBLite"), repos=c("http://dev.monetdb.org/Assets/R/", "http://cran.rstudio.com/"), type="binary", lib=installdir, quiet=T)

	sink(type = "message") 
}))

library(MonetDBLite, quietly=T, lib.loc=installdir)

stopifnot(!is.null(sessionInfo()$otherPkgs$MonetDBLite))
print("SUCCESS")
