# create lib folder if it should be missing to avoid the prompt
dir.create(unlist(strsplit(Sys.getenv("R_LIBS_USER"), .Platform$path.sep))[1L], recursive = TRUE, showWarnings=F)

# autoinstall DBI and digest, we need those to install MonetDB.R
dd <- capture.output(suppressMessages(suppressWarnings({
	install.packages(c("devtools", "digest"), repos="http://cran.rstudio.com/", quiet=T)
	devtools::install_github("rstats-db/DBI", quiet=T)
})))
