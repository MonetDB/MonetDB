\name{sd}
\alias{sd}
\alias{sd.monet.frame}

\title{
   Standard Deviation
}
\description{
  \code{sd} calculates the standard deviation for \code{monet.frame} objects.
}
\usage{
 	\method{sd}{monet.frame} (x, na.rm = FALSE)
}
\arguments{
  \item{x}{a \code{monet.frame} object with only a single column}
  \item{na.rm}{Whether to remove NA/NULL values before calculation}
  }
\value{
  Returns the standard deviation.
}
\seealso{
  \code{\link[stats]{sd}}
}
\examples{
\dontrun{
	frame <- mf("demo","iris")
	sd(frame$sepal_width)
}}
