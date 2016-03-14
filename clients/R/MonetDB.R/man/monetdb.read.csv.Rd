\name{monetdb.read.csv}
\alias{monetdb.read.csv}
\alias{monet.read.csv}

\title{
  Import a CSV file into MonetDB
}
\description{
  Instruct MonetDB to read a CSV file, optionally also create the table for it.
}
\usage{
   monetdb.read.csv (conn, files, tablename, header=TRUE, 
  locked=FALSE, best.effort=FALSE, na.strings="", nrow.check=500, delim=",",
  newline = "\\\\n", quote = "\"", create=TRUE, col.names=NULL, lower.case.names=FALSE, 
  sep=delim, ...)
}
\arguments{
  \item{conn}{A MonetDB.R database connection. Created using \code{\link[DBI]{dbConnect}} with the 
  \code{\link[MonetDB.R]{MonetDB.R}} database driver.}
  \item{files}{A single string or a vector of strings containing the absolute file names of the CSV files to be imported.}
  \item{tablename}{Name of the database table the CSV files should be imported in. Created if necessary.}
  \item{header}{Whether or not the CSV files contain a header line.}
  \item{locked}{Whether or not to disable transactions for import. 
  Setting this to TRUE can greatly improve the import performance.}
  \item{best.effort}{Use best effort flag when reading csv files and continue importing even if parsing of fields/lines fails.}
  \item{na.strings}{Which string value to interpret as \code{NA} value.}
  \item{nrow.check}{Amount of rows that should be read from the CSV when the 
  table is being created to determine column types.}
  \item{delim}{Field separator in CSV file.}
  \item{newline}{Newline in CSV file, usually \\n for UNIX-like systems and \\r\\r on Windows.}
  \item{quote}{Quote character(s) in CSV file.}
  \item{create}{Create table before importing?}
  \item{lower.case.names}{Convert all column names to lowercase in the database?}
  \item{col.names}{Optional column names in case the ones from CSV file should not be used}
  \item{sep}{alias for \code{delim}}
  \item{...}{Additional parameters. Currently not in use.}
}
 
\value{
  Returns the number of rows imported if successful.
}
\seealso{
  \code{dbWriteTable} in \code{\link[DBI]{DBIConnection-class}}
}
\examples{
\dontrun{
library(DBI)
# connect to MonetDB
conn <- dbConnect(MonetDB.R::MonetDB(), dbname = "demo")
# write test data to temporary CSV file
file <- tempfile()
write.table(iris, file, sep=",")
# create table and import CSV
MonetDB.R::monetdb.read.csv(conn, file, "iris")
}}
\keyword{interface}
