# create lib folder if it should be missing to avoid the prompt
dir.create(unlist(strsplit(Sys.getenv("R_LIBS_USER"), .Platform$path.sep))[1L], recursive = TRUE, showWarnings=F)

# autoinstall DBI and digest, we need those to install MonetDB.R
dd <- capture.output( suppressMessages ( {
	(function(lp) {
	np <- lp[!(lp %in% installed.packages()[,"Package"])]
	repos <- "http://cran.rstudio.com/"
	if(length(np)) install.packages(np,repos=repos, type="source", quiet=T)
	update.packages(repos=repos, ask=F, oldPkgs=lp, type="source", quiet=T)
	x <- lapply(lp,function(x){library(x,character.only=TRUE,quietly=T)}) 
	})(c("DBI", "digest"))
}))

