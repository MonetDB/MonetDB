# this script generates a C file containing the content of all mal/sql files 
# mentioned in Makefile.ag files as well as a mapping from file name to index
args <- commandArgs(trailingOnly = TRUE)
makefiles <- dir(path=args[1], 
	pattern="Makefile.ag", full.names=T, recursive=T)
con <- file(args[2], open="wb"); fidx <- character(0)
ct <- function(x, ...) cat(x, file=con, append=T, ...)
ct("char* include_file_data[] = {\n")
for (mf in makefiles) {
	instfiles <- stringr::str_extract_all(
		readChar(mf, file.info(mf)$size), "\\w+\\.(mal|sql)")[[1]]
	for (instf in file.path(dirname(mf), instfiles)) {
		ct("\"\\x")
		ct(readBin(instf, what="raw", n=file.info(instf)$size), sep="\\x")
		ct("\\0\",\n")
	}
	fidx <- c(fidx, instfiles)
}
ct("NULL};\nchar* include_file_name[] = {\"")
ct(as.character(fidx), sep="\",\"")
ct("\",NULL};\n")
