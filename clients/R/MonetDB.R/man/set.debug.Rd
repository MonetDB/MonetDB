\name{set.debug}
\alias{set.debug}

\title{
  Enables/Disables SQL query debugging on \code{monet.frame} objects.
}
\description{
\code{monet.frame} translates the user's operation into a string of SQL queries 
and executes some of them. Normally, these SQL queries are not printed, but this function 
can change that.
}
\usage{
   set.debug (x, debug )

}
\arguments{
  \item{x}{A \code{\link[MonetDB.R]{monet.frame}} monet.frame virtual data object.}
  \item{debug}{Boolean (TRUE/FALSE) to enable/disable debugging}
 }
\examples{
\dontrun{
	frame <- mf("demo","iris")
	set.debug(frame,TRUE)
}}
 