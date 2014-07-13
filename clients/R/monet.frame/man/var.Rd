\name{var}
\alias{var}
\alias{var.monet.frame}

\title{
   Variance
}
\description{
  \code{var} calculates the variance for \code{monet.frame} objects.
}
\usage{
 	\method{var}{monet.frame} (x, y = NULL, na.rm = FALSE, use)
}
\arguments{
  \item{x}{a \code{monet.frame} object with only a single column}
  \item{y}{Unsupported, must not be set}
  \item{na.rm}{Whether to remove NA/NULL values before calculation}
  \item{use}{Unsupported, must not be set}
    
  
  }
\value{
  Returns the variance
}
\seealso{
  \code{\link[stats]{cor}}
}

\examples{
\dontrun{
	frame <- mf("demo","iris")
	var(frame$sepal_width)
}}
