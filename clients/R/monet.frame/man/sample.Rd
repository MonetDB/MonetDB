\name{sample}
\alias{sample}
\alias{sample.monet.frame}

\title{
   Random Data Sampling
}
\description{
  \code{sample} gets a number of randomly selected values from the query result or 
  table wrapped by a \code{monet.frame} object.
}
\usage{
 	\method{sample}{monet.frame} (x, size, replace = FALSE, prob = NULL)
}
\arguments{
  \item{x}{a \code{monet.frame} object}
  \item{size}{Sample size, numerical value}
  \item{replace}{Unsupported, must not be set}
  \item{prob}{Unsupported, must not be set}
  }
\value{
  A \code{data.frame} object containing the sample rows.
}
\seealso{
  \code{\link[base]{sample}}
}
\examples{
\dontrun{
	frame <- mf("demo","iris")
	sample(frame,4)
}}
