\name{monetdbGetTransferredBytes}
\alias{monetdbGetTransferredBytes}

\title{
  Get and reset MAPI traffic counters
}
\description{
  The MonetDB.R connector keeps track on the amount of bytes transferred to and from the database. 
  The counters are reset after this function is called. This is a developer feature and is not 
  very useful to typical users.
}
\usage{
  monetdbGetTransferredBytes ()
}
\value{
  Returns a list with two entries, \code{bytes.in} and \code{bytes.out}.
}