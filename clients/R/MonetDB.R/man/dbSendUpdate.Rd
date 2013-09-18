\name{dbSendUpdate}
\alias{dbSendUpdate}
\alias{dbSendUpdateAsync}
\alias{dbSendUpdate,MonetDBConnection,character-method}
\alias{dbSendUpdateAsync,MonetDBConnection,character-method}

\title{
  Send a data-altering SQL statement to the database.
}
\description{
  \code{dbSendUpdate} is used to send a data-altering statement to a MonetDB database, 
  e.g. \code{CREATE TABLE} or \code{INSERT}. As a convenience feature, a placeholder 
  (\code{?} character) can be used in the SQL statement, and bound to parameters given 
  in the varargs group before execution. This is especially useful when scripting 
  database updates, since the parameters will be automatically quoted. 
  
  The \code{dbSendUpdateAsync} function is used when the database update is called from 
  finalizers, to avoid very esoteric concurrency problems. Here, the update is not guaranteed 
  to be immediately run. Also, the method returns immediately.
}
\usage{
  dbSendUpdate( conn, statement, ..., async=FALSE )
}
\arguments{
  \item{conn}{A MonetDB.R database connection. Created using \code{\link[DBI]{dbConnect}} 
   with the \code{\link[MonetDB.R]{MonetDB.R}} database driver.}
   \item{statement}{A SQL statement to be sent to the database, e.g. 'UPDATE' or 'INSERT'.}
   \item{...}{Parameters to be bound to '?' characters in the query, similar to JDBC.}
   \item{async}{Behave like \code{dbSendUpdateAsync}? Defaults to \code{FALSE}.}
 }
\value{
  Returns \code{TRUE} if the update was successful.
}
\seealso{
  \code{\link[DBI]{dbSendQuery}}
}
\examples{
\dontrun{
# connect to MonetDB
conn <- dbConnect(MonetDB.R(), "monetdb://localhost/acs")
# create table
dbSendUpdate(conn, "CREATE TABLE foo(a INT,b VARCHAR(100))")
# insert value, bind parameters to placeholders in statement
dbSendUpdate(conn, "INSERT INTO foo VALUES(?,?)", 42, "bar")

}}
\keyword{interface}
