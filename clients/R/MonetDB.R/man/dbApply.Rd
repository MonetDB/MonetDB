\name{mdbapply}
\alias{mdbapply}
\alias{mdbapply,MonetDBConnection-method}

\title{
  Apply a R function to a MonetDB table.
}
\description{
\code{dbApply} is used to switch the data from the normal auto-commiting mode into transactional mode. Here, changes to the database will not be permanent until \code{dbCommit} is called. If the changes are not to be kept around, you can use \code{dbRollback} to undo all the changes since \code{dbTransaction} was called.
  
}
\usage{
  mdbapply(conn, table, fun, ...)
}
\arguments{
  \item{conn}{A MonetDB.R database connection. Created using \code{\link[DBI]{dbConnect}} 
   with the \code{\link[MonetDB.R]{MonetDB.R}} database driver.}
   \item{table}{A MonetDB database table. Can also be a view or temporary table.}
   \item{fun}{A R function to be run on the database table. The function gets passed a single \code{data.frame} argument which represents the database table. The function needs to return a single vector (for now).}
   \item{...}{Other parameters to be passed to the function}

 }
\value{
  Returns the result of the function applied to the database table.
}
\examples{
\dontrun{
conn <- dbConnect(MonetDB.R(), "demo")
data(mtcars)
dbWriteTable(conn, "mtcars", mtcars)

mpgplus42 <- mdbapply(conn, "mtcars", "double", function(d) {
	d$mpg + 42
})
}}
