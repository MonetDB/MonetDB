# autoinstall stuff to test sqlsurvey and dplyr
dd <- capture.output(suppressMessages(suppressWarnings({
	(function(lp) {
	np <- lp[!(lp %in% installed.packages()[,"Package"])]
	repos <- c('http://download.r-forge.r-project.org','http://cran.rstudio.com/')
	if(length(np)) install.packages(np,repos=repos, type="source", quiet=T)
	update.packages(repos=repos, ask=F, oldPkgs=lp, type="source", quiet=T)
	x <- lapply(lp,function(x){library(x,character.only=TRUE,quietly=T)}) 
	})(c('Rcpp', 'dplyr','survey','sqlsurvey','Lahman','nycflights13'))
})))

