# autoinstall DBI and digest, we need those to install MonetDB.R
packages_required <- c("digest", "DBI")

cat("#~BeginProfilingOutput~#\n", file=stderr())
cat("#~BeginProfilingOutput~#\n", file=stdout())

install.packages(packages_required, repos="http://cran.rstudio.com/")

cat("#~EndProfilingOutput~#\n", file=stderr())
cat("#~EndProfilingOutput~#\n\n", file=stdout())

stopifnot(all(c(packages_required, "DBI") %in% installed.packages()[,"Package"]))
