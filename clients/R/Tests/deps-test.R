# autoinstall stuff to test sqlsurvey and dplyr
dd <- capture.output(suppressMessages(suppressWarnings({
	(function(lp) {
	np <- lp[!(lp %in% installed.packages()[,"Package"])]
	repos <- c('http://download.r-forge.r-project.org','http://cran.rstudio.com/')
	if(length(np)) install.packages(np,repos=repos, quiet=T)
	update.packages(repos=repos, ask=F, oldPkgs=lp, quiet=T)
	})(c('Rcpp', 'dplyr','survey','sqlsurvey','Lahman','nycflights13'))
})))
