basedir <- Sys.getenv("TSTTRGDIR")
srcdir <- Sys.getenv("TSTSRCDIR")
if (basedir == "" || srcdir == "") {
	stop("Need TSTTRGDIR and TSTSRCDIR environment vars")
}

builddir   <- file.path(basedir, "rbuilddir")
installdir <- file.path(basedir, "rlibdir")
dir.create(builddir)
if (file.exists(installdir)) unlink(installdir, recursive=T)
dir.create(installdir)
file.copy(from=file.path(srcdir, "..", "MonetDB.R"), to=builddir, recursive=T)
dd <- capture.output(suppressMessages( {
	sink(file=file(tempfile(), open = "wt"), type = "message") 
	install.packages(file.path(builddir, "MonetDB.R"), repos=NULL, lib=installdir, quiet=T)
	sink(type = "message") 
}))
library(MonetDB.R,quietly=T,lib.loc=installdir)
