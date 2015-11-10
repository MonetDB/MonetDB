\name{MonetDBLite}
\alias{MonetDBLite}
\alias{RMonetDBLite}
\alias{monetdblite}
\alias{rmonetdblite}

\title{
  MonetDBLite DBI driver
}
\description{
  \code{MonetDBLite} creates a new DBI driver to interact with MonetDBLite
}
\usage{
  MonetDBLite()
}
\value{
  Returns a MonetDBLite driver object that can be used in calls to
  \code{\link[DBI]{dbConnect}}.
}
\details{
  The \code{MonetDB.R} function creates the R object which can be used to a
  call \code{\link[DBI]{dbConnect}} which actually creates the connection. 
  Since it has no parameters, it is most commonly used inline with the \code{\link[DBI]{dbConnect}} call.
}
\examples{
\dontrun{
conn <- dbConnect(MonetDBLite(), tempdir())

}}
