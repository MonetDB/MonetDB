# autoinstall DBI and digest, we need those to install MonetDB.R

cat("#~BeginProfilingOutput~#\n", file=stderr())
cat("#~BeginProfilingOutput~#\n", file=stdout())

repos <- "http://cran.rstudio.com/"
packages_required <- c("devtools", "digest")
install.packages(packages_required, repos=repos, quiet=T)
update.packages(repos=repos, ask=F, oldPkgs=packages_required)
devtools::install_github("rstats-db/DBI", quiet=T)

cat("#~EndProfilingOutput~#\n", file=stderr())
cat("#~EndProfilingOutput~#\n\n", file=stdout())

stopifnot(all(c(packages_required, "DBI") %in% installed.packages()[,"Package"]))
