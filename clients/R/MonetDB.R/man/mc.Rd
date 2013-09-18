\name{mc}
\alias{mc}

\title{
   Shorthand connection constructor for MonetDB
}
\description{
  \code{mc(...)} provides a short way of connecting to a MonetDB database. It is equivalent to \code{dbConnect(MonetDB.R(),...)}
}
\usage{
mc(dbname="demo", user="monetdb", password="monetdb", host="localhost", port=50000, 
  timeout=86400, wait=FALSE,...) 
}
\arguments{
  \item{dbname}{Database name}
  \item{user}{Username for database}
  \item{password}{Password for database}
  \item{host}{Host name of database server}
  \item{port}{TCP Port number of database server}
  \item{timeout}{Database connection and query timeout}
  \item{wait}{Wait for DB to become available or not}
  \item{...}{Unused}

  }
\value{
  Returns a DBI connection to the specified MonetDB database.
}

\seealso{
  \code{\link[DBI]{dbConnect}}
}
\examples{
\dontrun{
	con <- mc(dbname="demo",hostname="localhost")
}}
