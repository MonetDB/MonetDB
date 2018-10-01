packages_required <- c("assertthat","testthat","survey","nycflights13","RSQLite","dbplyr","dplyr","gdata","callr","DBItest")

install_or_upgrade_packages <- function(lp) {
	np <- lp[!(lp %in% installed.packages()[,"Package"])]
	repos <- 'http://cran.rstudio.com/'
	if(length(np)) install.packages(np, repos=repos, quiet=T)
	update.packages(repos=repos, ask=F, oldPkgs=lp, quiet=T)
}


cat("#~BeginProfilingOutput~#\n", file=stderr())
cat("#~BeginProfilingOutput~#\n", file=stdout())

install_or_upgrade_packages(packages_required)

cat("#~EndProfilingOutput~#\n", file=stderr())
cat("#~EndProfilingOutput~#\n", file=stdout())

stopifnot(all(packages_required %in% installed.packages()[,"Package"]))
