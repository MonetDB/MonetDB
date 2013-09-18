\name{tabulate}
\alias{tabulate}
\alias{tabulate.default}

\title{
   \code{tabulate()} support for \code{monet.frame} objects
}
\description{
  Since \code{tabulate} is not generic, this is changed by our package. Also, we provide an implementation
  of the function for \code{monet.frame} objects.
}
\usage{
 	\method{tabulate}{default}(bin, nbins = max(1, bin, na.rm = TRUE))
}
\arguments{
  \item{bin}{a \code{monet.frame} object with only a single column.}
  \item{nbins}{the number of bins to be used.}
  }
\value{
	A frequency table for the given value
}
\seealso{
  \code{\link[base]{tabulate}}
}