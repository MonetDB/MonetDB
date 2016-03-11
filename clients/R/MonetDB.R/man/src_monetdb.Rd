\name{src_monetdb}
\alias{src_monetdb}
\alias{tbl.src_monetdb}
\alias{src_desc.src_monetdb}
\alias{src_translate_env.src_monetdb}
\alias{sample_frac.tbl_monetdb}
\alias{sample_n.tbl_monetdb}
\alias{db_analyze.MonetDBConnection}
\alias{db_create_index.MonetDBConnection}
\alias{db_insert_into.MonetDBConnection}
\alias{db_query_fields.MonetDBConnection}
\alias{db_query_rows.MonetDBConnection}
\alias{db_query_fields.MonetDBEmbeddedConnection}
\alias{db_query_rows.MonetDBEmbeddedConnection}
\alias{db_save_query.MonetDBConnection}
\alias{sql_subquery.MonetDBConnection}

\title{dplyr integration from MonetDB.R}
\usage{
src_monetdb(dbname, host = "localhost", port = 50000L, user = "monetdb",
  password = "monetdb", con=FALSE, ...)

\method{tbl}{src_monetdb}(src, from, ...)
}
\arguments{
\item{dbname}{Database name}

\item{host,port}{Host name and port number of database (defaults to localhost:50000)}

\item{user,password}{User name and password (if needed)}

\item{con}{Existing DBI connection to MonetDB to be re-used}

\item{...}{for the src, other arguments passed on to the underlying
database connector, \code{dbConnect}.}

\item{src}{a MonetDB src created with \code{src_monetdb}.}

\item{from}{Either a string giving the name of table in database, or SQL described a derived table or compound join.}
}
\description{
Use \code{src_monetdb} to connect to an existing MonetDB database,
and \code{tbl} to connect to tables within that database. Please note that the ORDER BY, LIMIT and OFFSET keywords
are not supported in the query when using \code{tbl} on a connection to a MonetDB database.
If you are running a local database, you only need to define the name of the database you want to connect to.
}

\examples{
\dontrun{
library(dplyr)
# Connection basics ---------------------------------------------------------
# To connect to a database first create a src:
my_db <- MonetDB.R::src_monetdb(dbname="demo")
# Then reference a tbl within that src
my_tbl <- tbl(my_db, "my_table")
}
}

