dd <- capture.output(suppressMessages(suppressWarnings({
	(function(lp) {
	np <- lp[!(lp %in% installed.packages()[,"Package"])]
	repos <- 'http://cran.rstudio.com/'
	if(length(np)) install.packages(np, repos=repos, quiet=T)
	update.packages(repos=repos, ask=F, oldPkgs=lp, quiet=T)
	})(c('Rcpp', 'dplyr', 'Lahman', 'nycflights13'))
})))
