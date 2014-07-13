\name{control}
\alias{control}
\alias{monetdb.server.setup}
\alias{monetdb.server.start}
\alias{monetdb.server.stop}

\title{
  Control a MonetDB server from the R shell.
}
\description{
The MonetDB server can be controlled from the R shell using the functions described below. 
The general process is to generate a MonetDb database directory and startup script using 
\code{monetdb.server.setup}, then pass the path to the startup script to \code{monetdb.server.start}. 
This function will return the process id of the database server, which in turn can be passed to 
\code{monetdb.server.stop} to stop the database server again. 
}
\usage{
monetdb.server.setup(database.directory,monetdb.program.path, 
	dbname = "demo", dbport = 50000)
monetdb.server.start(bat.file)
monetdb.server.stop(correct.pid)
}
\arguments{
  \item{database.directory}{Path to the directory where the initialization script and all data will be stored. Must be empty or non-existant.}
   \item{monetdb.program.path}{Path to the MonetDB installation}
   \item{dbname}{Database name to be created}
   \item{dbport}{TCP port for MonetDB to listen for connections. This port should not conflict with other running programs on your local computer. Two databases with the same port number cannot be accessed at the same time}
   \item{bat.file}{Path to the MonetDB startup script. This path is returned by \code{monetdb.server.setup}}
   \item{correct.pid}{Process ID of the running MonetDB server. This number is returned by \code{monetdb.server.start}}
 }
\value{
  \code{monetdb.server.setup} returns the path to a MonetDB startup script, which can used many times
  \code{monetdb.server.start} returns the process id of the MonetDB database server
}

\examples{
\dontrun{
startscript <- monetdb.server.setup("/tmp/database","/usr/local/monetdb/", "db1", 50001)
pid <- monetdb.server.start(startscript)
monetdb.server.stop(pid)
conn <- dbConnect(MonetDB.R(), "monetdb://localhost:50001/db1")
}}
