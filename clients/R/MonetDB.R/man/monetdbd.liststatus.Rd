\name{monetdbd.liststatus}
\alias{monetdbd.liststatus}

\title{
  Get list of available databases from monetdbd
}
\description{
  The \code{monetdbd} daemon can be used to manage multiple MonetDB databases in UNIX-like systems. This function connects to it and retrieves information about the available databases. Please note that \code{monetdbd} has to be configured to allow TCP control connections first. This can be done by setting a passphrase, e.g. "examplepassphrase" (\code{monetdbd set passphrase=examplepassphrase /path/to/dbfarm}) and then switching on remote control (\code{monetdbd set control=true /path/to/dbfarm}). 
}
\usage{
  monetdbd.liststatus(passphrase, host="localhost", port=50000L, timeout=86400L)
}
\arguments{
  \item{passphrase}{\code{monetdbd} passphrase, see description}
  \item{host}{hostname to connect to}
  \item{port}{TCP port where \code{monetdbd} listens}
  \item{timeout}{Connection timeout (seconds)}
 }
 
\value{
  A \code{data.frame} that contains various information about the available databases.
}
\examples{
\dontrun{
print(monetdbd.liststatus("mypasshprase")$dbname)
}}
