dd <- capture.output(suppressMessages(suppressWarnings({
	(function(lp) {
	np <- lp[!(lp %in% installed.packages()[,"Package"])]
	repos <- c('http://dev.monetdb.org/Assets/R/','http://download.r-forge.r-project.org','http://cran.rstudio.com/')
	if(length(np)) install.packages(np,repos=repos, type="source", quiet=T)
	update.packages(repos=repos, ask=F, oldPkgs=lp, type="source", quiet=T)
	x <- lapply(lp,function(x){library(x,character.only=TRUE,quietly=T)}) 
	})(c('testthat', 'MonetDB.R', 'survey'))
})))
