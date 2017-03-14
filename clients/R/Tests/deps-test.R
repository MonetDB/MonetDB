packages_required <- c('Rcpp', 'dplyr', 'Lahman', 'nycflights13', 'gdata', 'survey')

install_or_upgrade_packages <- function(lp) {
	np <- lp[!(lp %in% installed.packages()[,"Package"])]
	repos <- 'http://cran.rstudio.com/'
	if(length(np)) install.packages(np, repos=repos, quiet=T)
	update.packages(repos=repos, ask=F, oldPkgs=lp, quiet=T)

	# dev dplyr
	if (packageVersion("devtools") < 1.6) {
	  install.packages("devtools")
	}
	devtools::install_github("hadley/lazyeval")
	devtools::install_github("hadley/dplyr")

}


cat("#~BeginProfilingOutput~#\n", file=stderr())
cat("#~BeginProfilingOutput~#\n", file=stdout())

devtools::install_github("hannesmuehleisen/MonetDBLite", quiet=T)

install_or_upgrade_packages(packages_required)

cat("#~EndProfilingOutput~#\n", file=stderr())
cat("#~EndProfilingOutput~#\n", file=stdout())

stopifnot(all(packages_required %in% installed.packages()[,"Package"]))
