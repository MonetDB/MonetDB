\name{dbTransaction}
\alias{dbTransaction}
\alias{dbTransaction,MonetDBConnection-method}
\alias{dbCommit,MonetDBConnection-method}
\alias{dbRollback,MonetDBConnection-method}

\title{
  Create, commit or abort a database transaction.
}
\description{
\code{dbTransaction} is used to switch the data from the normal auto-commiting mode into transactional mode. Here, changes to the database will not be permanent until \code{dbCommit} is called. If the changes are not to be kept around, you can use \code{dbRollback} to undo all the changes since \code{dbTransaction} was called.
  
}
\usage{
  dbTransaction(conn, ...)
}
\arguments{
  \item{conn}{A MonetDB.R database connection. Created using \code{\link[DBI]{dbConnect}} 
   with the \code{\link[MonetDB.R]{MonetDB.R}} database driver.}
   \item{...}{Future use.}
 }
\value{
  Returns \code{TRUE} if the transaction command was successful.
}
\examples{
\dontrun{
conn <- dbConnect(MonetDB.R(), "monetdb://localhost/acs")
dbSendUpdate(conn, "CREATE TABLE foo(a INT,b VARCHAR(100))")
dbTransaction(conn)
dbSendUpdate(conn, "INSERT INTO foo VALUES(?,?)", 42, "bar")
dbCommit(conn)
dbTransaction(conn)
dbSendUpdate(conn, "INSERT INTO foo VALUES(?,?)", 43, "bar")
dbRollback(conn)

# only 42 will be in table foo
}}
