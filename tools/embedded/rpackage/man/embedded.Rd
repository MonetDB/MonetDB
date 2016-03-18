\name{embedded}
\alias{monetdb_embedded_startup}
\alias{monetdb_embedded_query}
\alias{monetdb_embedded_append}
\alias{monetdb_embedded_connect}
\alias{monetdb_embedded_disconnect}
\alias{monetdb_embedded_shutdown}

\title{
  MonetDBLite low-level interface
}
\description{
Low-level API for MonetDBLite. \code{DBI}/\code{dplyr} frontends for these functions is in the \code{MonetDB.R} package. This package is not really meant to be used directly.
}
\usage{
monetdb_embedded_startup(dir=tempdir(), quiet = TRUE, sequential = TRUE)
monetdb_embedded_connect()
monetdb_embedded_query(conn, query, execute = TRUE, resultconvert = TRUE)
monetdb_embedded_append(conn, table, tdata, schema = "sys")
monetdb_embedded_disconnect(conn)
monetdb_embedded_shutdown()
}
\arguments{
  \item{dir}{Path to the directory where the database will be created. Must be writeable or createable.}
   \item{quiet}{Suppress MonetDB startup messages?}
   \item{sequential}{Disable intra-query parallelization}
   \item{conn}{Connection as obtained by calling \code{monetdb_embedded_connect()}}
   \item{query}{SQL query, DDL statement or transaction statement}
   \item{execute}{Actually execute query?}
   \item{resultconvert}{Convert results to R vectors?}
   \item{table}{Database table name}
   \item{tdata}{\code{data.frame} with columns matching the schema of \code{table}}
  \item{schema}{Schema name}
 }
\value{
  \code{monetdb_embedded_startup} returns \code{TRUE} if successful.
  \code{monetdb_embedded_query} returns a list with query results which might contain a \code{data.frame} with results or an error message string.
  \code{monetdb_embedded_query} returns \code{TRUE} if successful or a string with an error message if not.
}

\examples{
monetdb_embedded_startup(tempdir())
conn <- monetdb_embedded_connect()
monetdb_embedded_query(conn, "SELECT * FROM tables;")
monetdb_embedded_query(conn, "CREATE TABLE foo(i INTEGER, j INTEGER)")
monetdb_embedded_append(conn, "foo", data.frame(i=1:10, j=21:30))
monetdb_embedded_disconnect(conn)
monetdb_embedded_shutdown()
}
