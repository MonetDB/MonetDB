\name{mq}
\alias{mq}

\title{
   Connect to a database, run a single SELECT query, and disconnect again.
}
\description{
  \code{mq(...)} provides a short way to connect to a MonetDB database, run a single SELECT query, and disconnect again.
}
\usage{
mq(dbname, query, ...) 
}
\arguments{
  \item{dbname}{Database name}
  \item{query}{SQL SELECT query to run}
  \item{...}{Other options for \code{\link[DBI]{dbConnect}}}
  }
\value{
  Returns a data frame that contains the result of the passed query or an error if something went wrong.
}

\seealso{
  \code{\link[DBI]{dbConnect}}
  \code{\link[MonetDB.R]{mc}}
}
\examples{
\dontrun{
  mq("demo","SELECT 1")
}}
