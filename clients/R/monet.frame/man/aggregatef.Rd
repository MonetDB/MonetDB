\name{aggregatef}
\alias{aggregatef}

\title{
   Aggregate by formula
}
\description{
  \code{aggregatef} is a standard formula interface to \code{aggregate.monet.frame}
}
\usage{
 	aggregatef (formula, data, FUN, \dots, subset, na.action = na.omit)
}
\arguments{
  \item{formula}{\code{formula} object for aggregation}
  \item{data}{\code{monet.frame} object}
  \item{FUN}{Aggregation function}  
  \item{\dots}{further arguments passed to or used by methods}
  \item{subset}{an optional vector specifying a subset of observations to be used (Unused here)}
  \item{na.action}{a function which indicates what should happen when the data contain \code{NA} values (Unused here)}
}
\value{
  A \code{monet.frame} object with aggregated values
}
\seealso{
  \code{\link[stats]{aggregate}}
  
}