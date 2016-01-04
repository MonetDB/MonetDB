# this script generates startup createdb scripts for MonetDBlite
args <- commandArgs(trailingOnly = TRUE)

dp <- function(fn, fl, sn) {
	con <- file(fn, open="wb");
	ct <- function(x, ...) cat(x, file=con, append=T, ...)
	ct("char*")
	ct(sn)
	ct(" = \"")
	invisible(lapply(fl, function(mf) {
		ct("\\x")
		message("Including ", mf, " into ", fn)
		ct(readBin(mf, what="raw", n=file.info(mf)$size), sep="\\x")
		ct("\\n")
	}))
	ct("\";\n")
	close(con)
}

# find all include'd modules in mal_init
mal_init <- file.path(args[1], "mal_init.mal")
mal_init_modules <- stringr::str_match_all(readChar(mal_init, file.info(mal_init)$size), 
	"include (\\w+);")[[1]][,2]

# modules we don't load.
ignored_modules <- c("autoload", "mcurl", "sabaoth", "recycle", "remote", "txtsim", 
	"tokenizer", "zorder", "srvpool", "mal_mapi")

# make sure files exist and male full paths
mal_init_modules <- file.path(args[1], paste0(setdiff(mal_init_modules, ignored_modules), ".mal"))
mal_init_modules <- mal_init_modules[file.exists(mal_init_modules)]

# scan autoload directory and add those too
autoload <- sort(dir(path=file.path(args[1], "autoload"), pattern="*.mal", full.names=T))
mal_init_modules <- c(mal_init_modules, autoload)

# dump everything into header file
mal_init_file <- file.path(args[2], "monetdb5", "mal", "mal_init_inline.h")
dp(mal_init_file, mal_init_modules, "mal_init_inline")

# do the same for sql createdb
createdb <- sort(dir(path=file.path(args[1], "createdb"), pattern="*.sql", full.names=T))
createdb_file <- file.path(args[2], "sql", "backends", "monet5", "createdb_inline.h")
dp(createdb_file, createdb, "createdb_inline")
