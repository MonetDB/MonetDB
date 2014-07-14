\name{tabulate.monet.frame}
\alias{tabulate.monet.frame}

\title{
   \code{tabulate()} support for \code{monet.frame} objects
}
\description{
  Implementation
  of the \code{tabulate()} function for \code{monet.frame} objects.
}
\usage{
 	\method{tabulate}{monet.frame} (bin, nbins = max(bin))
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

