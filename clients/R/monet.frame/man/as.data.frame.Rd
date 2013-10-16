\name{as.data.frame}
\alias{as.data.frame}
\alias{as.data.frame.monet.frame}
\alias{adf.monet.frame}
\alias{adf}

\alias{as.vector}
\alias{as.vector.monet.frame}
\alias{av.monet.frame}
\alias{av}

\title{
  Convert \code{monet.frame} into \code{data.frame}.
}
\description{
  Materialise virtual data object \code{monet.frame} into R objects \code{data.frame} or \code{vector}.
}
\usage{
   \method{as.data.frame}{monet.frame} (x, row.names, optional, warnSize=TRUE, \dots )

}
\arguments{
  \item{x}{A \code{\link[MonetDB.R]{monet.frame}} monet.frame virtual data object.}
   \item{row.names}{Unused.}
   \item{optional}{Unused.}
   \item{warnSize}{Prevent user from loading too many tuples from the DB. The limit is 
   calculated by multiplying \code{nrow} with \code{ncol}. If set to false, the warning 
   is disabled. The current limit is \code{10.000.000}.}
   \item{\dots}{Unused.}
 }
\value{
  returns a \code{\link[base]{data.frame}} or vector populated with data from the database table 
  that was wrapped by \code{monet.frame}. Note that \code{as.vector} can only be used on single-column 
  \code{moent.frame} objects.
}

\examples{
\dontrun{
# connect to MonetDB
conn <- dbConnect(MonetDB.R(), "monetdb://localhost/demo")
# write some test data
data(iris)
dbWriteTable(conn, "iris", iris)
# construct monet.frame
frame <- monet.frame(conn,"iris")
# convert to R data.frame
rframe <- as.data.frame(frame)
rvector <- as.vector(frame$sepal_width)

}}
\keyword{interface}
