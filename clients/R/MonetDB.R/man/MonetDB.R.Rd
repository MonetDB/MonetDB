\name{MonetDB.R}
\alias{MonetDB.R-package}
\alias{MonetDB.R}
\alias{MonetDBR}
\alias{MonetDB}
\alias{MonetR}

\title{
  DBI database connector and virtual data object for MonetDB
}
\description{
  \code{MonetDB.R} creates a new DBI driver that can be used to connect and interact with MonetDB.
}
\usage{
  MonetDB.R ()
}
\value{
  Returns a driver object that can be used in calls to
  \code{\link[DBI]{dbConnect}}.
}
\details{
  The \code{MonetDB.R} function creates the R object which can be used to a
  call \code{\link[DBI]{dbConnect}} which actually creates the connection. 
  Since it has no parameters, it is most commonly used inline with the \code{\link[DBI]{dbConnect}} call.
 
  This package aims to provide a reasonably complete implementation of the DBI. 
  A number of additional methods are provided: \code{\link{dbSendUpdate}} for database-altering 
  statements, \code{\link{dbSendUpdateAsync}} for cleanup operations and 
  \code{\link{monetdb.read.csv}} for database CSV import.
}
\seealso{
  \code{\link[DBI]{dbConnect}} for documentation how to invoke the driver
  
  \code{\link[MonetDB.R]{monet.frame}} for a \code{data.frame()}-like interface to the DB  
  
  \code{\link[=control]{monetdb.server.setup}} to set up and start a local MonetDB server from R
}
\examples{
\dontrun{
conn <- dbConnect(MonetDB.R(), "monetdb://localhost/demo")
dbListTables(conn)
data(iris)
dbWriteTable(conn, "iris", iris)
dbGetQuery(conn, "SELECT COUNT(*) FROM iris;")
d <- dbReadTable(conn, "iris")

}}
\keyword{interface}
