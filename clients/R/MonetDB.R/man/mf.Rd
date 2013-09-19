\name{mf}
\alias{mf}

\title{
   Shorthand constructor for \code{monet.frame} objects
}
\description{
  \code{mf} provides a short way of connecting to a MonetDB database and then constructs a \code{monet.frame} object for the given table or query.
}
\usage{
 	mf (database,table,host="localhost",port=50000,user="monetdb",
    pass="monetdb",debug=FALSE,timeout=100) 

}
\arguments{
  \item{database}{Database name}
  \item{table}{Table name}
  \item{host}{Host name of database server}
  \item{port}{TCP Port number of database server}
  \item{user}{Username for database}
  \item{pass}{Password for database}
  \item{debug}{Debug mode}
  \item{timeout}{Database connection and query timeout}

 }
\value{
  Returns a \code{monet.frame} virtual data object for the specified table for the specified database.
}

\seealso{
  \code{\link[MonetDB.R]{monet.frame}}
}
\examples{
\dontrun{
	frame <- mf("demo","iris")
}}
