\name{monetdb_queryinfo}
\alias{monetdb_queryinfo}

\title{
  Get information about the result set of a query without actually executing it. This is mainly needed for \code{dplyr} compatibility.
}
\description{
  \code{monetdb_queryinfo(...)} is used to get the expected result set structure (# rows, # columns, column names, column types etc.) without actually running the query.
}
\usage{
monetdb_queryinfo(conn, query) 
}
\arguments{
  \item{conn}{Database name}
  \item{query}{SQL SELECT query to get information about}
  }
\value{
  Environment with various entries, e.g.
 \itemize{
  \item \code{cols} -- number of columns
  \item \code{rows} -- number of rows
  \item \code{types} -- vector of column type from database (e.g. "VARCHAR" or "INT")
  \item \code{names} -- vector of column names
  \item \code{tables} -- vector of table names
 }
}

\examples{
\dontrun{
  monetdb_queryinfo("demo","SELECT 1")
}}
