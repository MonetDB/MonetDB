dd <- capture.output(suppressMessages( {
	devtools::install_github("hannesmuehleisen/MonetDBLite", quiet=T)
}))
library(MonetDBLite,quietly=T)
