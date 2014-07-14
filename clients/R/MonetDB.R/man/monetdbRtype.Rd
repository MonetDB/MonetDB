\name{monetdbRtype}
\alias{monetdbRtype}

\title{
  Get the name of the R data type for a database type.
}
\description{
   For a database data type, get the name of the R data type it is being translated to.
}
\usage{
  monetdbRtype ( dbType )
}
\arguments{
  \item{dbType}{A database type string such as \code{CHAR} or \code{INTEGER}.}
 }
 
\value{
  String containing the R data type for the DB data type, e.g. \code{character} or \code{numeric}.
}