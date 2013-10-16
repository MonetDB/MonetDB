\name{as.list}
\alias{as.list}
\alias{as.list.monet.frame}

\title{
  Convert \code{monet.frame} into a R \code{list} of columns.
}
\description{
  Converts a \code{monet.frame} object that wraps a database table or query result 
  into a list of individual \code{monet.frame} objects for each column. 
  This is only here to support \code{summary} code.
}
\usage{
   \method{as.list}{monet.frame} (x, \dots )

}
\arguments{
  \item{x}{A \code{\link[MonetDB.R]{monet.frame}} monet.frame virtual data object.}
   \item{\dots}{Unused.}
 }
\value{
  returns a list of individual \code{monet.frame} objects for each column in the input object.
}

