options(warn = -1)
cat("#~BeginProfilingOutput~#\n", file=stderr())
cat("#~BeginProfilingOutput~#\n", file=stdout())

install_submodule_git <- function(x, ...) {
  install_dir <- tempfile()
  system(paste("git clone --recursive --depth 1", shQuote(x), 
shQuote(install_dir)))
  devtools::install(install_dir, ...)
}
install_submodule_git("https://github.com/hannesmuehleisen/MonetDBLite-R")

cat("#~EndProfilingOutput~#\n", file=stderr())
cat("#~EndProfilingOutput~#\n", file=stdout())

stopifnot("MonetDBLite" %in% installed.packages()[,"Package"])
library(MonetDBLite, quietly=T)
