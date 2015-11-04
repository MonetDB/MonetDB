\name{mclient}
\alias{mclient}

\title{
   Run a database shell
}
\description{
  \code{mclient} provides a shell-like database interface.
}
\usage{
mclient(conn, n=20)
}
\arguments{
  \item{conn}{Database connection}
  \item{n}{Number of rows to show in result sets}
  }
\value{
  No return value to speak of
}

\examples{
\dontrun{
  conn <- dbConnect(MonetDB.R, dbname="demo")
  mclient(conn)
}}
