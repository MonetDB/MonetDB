# autoinstall DBI and digest, we need those to install MonetDB.R
dd <- capture.output(suppressMessages(suppressWarnings({
	repos <- "http://cran.rstudio.com/"
	lp <- c("devtools", "digest")
	install.packages(lp, repos=repos, quiet=T)
	update.packages(repos=repos, ask=F, oldPkgs=lp)
	devtools::install_github("rstats-db/DBI", quiet=T)
})))
