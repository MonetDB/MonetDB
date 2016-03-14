\name{sqlite-compatibility}
\alias{sqlite-compatibility}
\alias{initExtension}
\alias{initExtension,MonetDBConnection-method}
\alias{isIdCurrent}
\alias{isIdCurrent,MonetDBConnection-method}
\alias{isIdCurrent,MonetDBResult-method}

\title{
  Compatibility functions for RSQlite
}
\description{
Some functions that RSQlite has and that we support to allow MonetDBLite being used as a drop-in replacement.
}
\usage{
isIdCurrent(dbObj, ...)
initExtension(dbObj, ...)
}
\arguments{
  \item{dbObj}{A MonetDB.R database connection. Created using \code{\link[DBI]{dbConnect}} with the 
    \code{\link[MonetDB.R]{MonetDB.R}} database driver.}
  \item{...}{Additional parameters. Currently not in use.}
}
