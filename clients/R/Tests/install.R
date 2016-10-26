cat("#~BeginProfilingOutput~#\n", file=stderr())
cat("#~BeginProfilingOutput~#\n", file=stdout())

devtools::install_github("hannesmuehleisen/MonetDBLite", quiet=T)

cat("#~EndProfilingOutput~#\n", file=stderr())
cat("#~EndProfilingOutput~#\n", file=stdout())

stopifnot("MonetDBLite" %in% installed.packages()[,"Package"])

library(MonetDBLite, quietly=T)
