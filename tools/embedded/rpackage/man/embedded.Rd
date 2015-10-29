\name{embedded}
\alias{monetdb_embedded_startup}
\alias{monetdb_embedded_query}
\alias{monetdb_embedded_append}

\title{
  Control embedded MonetDB
}
\description{
TODO
}
\usage{
monetdb_embedded_startup(dir=tempdir(), quiet=TRUE)
monetdb_embedded_query(conn, query, notreally=FALSE)
monetdb_embedded_append(conn, table, tdata, schema="sys")
monetdb_embedded_connect()
monetdb_embedded_disconnect(conn)
}
\arguments{
  \item{dir}{Path to the directory where the database will be created. Must be writeable or createable.}
   \item{quiet}{Suppress MonetDB startup messages?}
   \item{conn}{Connection as obtained by calling \code{monetdb_embedded_connect()}}
   \item{query}{SQL query, DDL statement or transaction statement}
   \item{notreally}{Skip result conversion?}
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
monetdb_embedded_query("SELECT * FROM tables;")
monetdb_embedded_query("CREATE TABLE foo(i INTEGER, j INTEGER)")
monetdb_embedded_append("foo", data.frame(i=1:10, j=21:30))
}
