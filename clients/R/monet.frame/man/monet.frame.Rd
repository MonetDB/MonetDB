\name{monet.frame}
\alias{monet.frame}
\alias{monetframe}


\title{
  Virtual R data object for MonetDB tables and query results
}
\description{
  \code{monet.frame} wraps a MonetDB table or query result and behaves like a R \code{\link[base]{data.frame}}. 
}
\usage{
  monet.frame( conn, tableOrQuery, debug=FALSE )
}
\arguments{
  \item{conn}{A MonetDB.R database connection. Created using \code{\link[DBI]{dbConnect}} with the 
  \code{\link[MonetDB.R]{MonetDB.R}} database driver.}
  \item{tableOrQuery}{A string either naming a database table or a SQL query as a basis for the new object.}
  \item{debug}{Logical value indicating whether the SQL queries being rewritten in the background are printed or not.}

 }
\value{
  Returns a \code{monet.frame} virtual data object that behaves similar to a \code{\link[base]{data.frame}}.
}
\details{
  \code{monet.frame} wraps a MonetDB table or query result and behaves like a R \code{\link[base]{data.frame}}.
  Many operations typical for \code{data.frame} are supported, e.g. \code{$}, \code{[}, basic math and comparisons.
  
  Data is only loaded when \code{\link[MonetDB.R]{as.data.frame}} or \code{as.vector} are called, 
  and most basic operations and calculations are mapped to SQL statements. 
  This greatly reduces the amount of data that has to be transferred between MonetDB and R 
  and thereby also increases speed, allowing you to analyse even more data with R.
  
  So far, the following user-facing R functions are implemented by \code{monet.frame} objects. The signatures and semantics 
  of these methods closely follow those implemented for \code{data.frame} objects.
    
     \itemize{
         \item \code{\link[MonetDB.R]{mf}} -- Shorthand constructor, also creates a DB connection
         \item \code{\link[MonetDB.R]{as.data.frame}} -- DB table or query result to R \code{data.frame} object
         \item \code{\link[=as.data.frame]{as.vector}} -- Single DB column to R \code{vector}
         \item \code{\link[utils]{str}} -- Display object structure (SQL query, columns, data types, ..)
         \item \code{\link[base]{print}} -- Prints database content wrapped in object to screen)
         \item \code{\link[base]{summary}} -- Calculates summaries on wrapped database content)
         \item \code{\link[base]{names}} -- Gets the column names of the wrapped query result or table as vector
         \item \code{\link[base]{dim}} -- Gets the dimension (#rows/#cols) of the wrapped query result or table
         \item \code{\link[base]{length}} -- Gets the number of columns of the wrapped query result or table
         \item \code{\link[stats]{na.fail}} -- Raise an error if any value is NA/NULL (only for single columns)
         \item \code{\link[utils]{head}} -- Gets the first n rows from the wrapped query result or table as \code{data.frame}
         \item \code{\link[=head]{tail}} -- Gets the last n rows from the wrapped query result or table as \code{data.frame}
         \item \code{\link[=Extract]{[}} -- Extracts specific rows and columns from the wrapped object, behaves like \code{[} on \code{data.frame}
         \item \code{\link[=Extract]{$}} -- Extracts a single column from the wrapped object, behaves like \code{$} on \code{data.frame}
         \item \code{\link[base]{subset}} -- Returns a subset of the wrapped table that meets filter conditions
         \item \code{\link[=na.fail]{na.omit}} -- Filters out NA/NULL values (only for single columns)
         \item \code{\link[MonetDB.R]{sample}} -- Gets n randomly selected rows of the wrapped query result or table as \code{data.frame}
          
         \item {\link[=groupGeneric]{S3 Group Generic Functions}}
         \itemize{
         	\item \code{abs}, \code{sign}, \code{sqrt}, \code{floor}, \code{ceiling}, \code{trunc}, \code{round}, \code{signif}
         	\item \code{exp}, \code{log}, \code{expm1}, \code{log1p}, \code{cos}, \code{sin}, \code{tan}, \code{acos}, \code{asin}, \code{atan}, \code{cosh}, \code{sinh}, \code{tanh}, \code{acosh}, \code{asinh}, \code{atanh}
         	\item \code{"+"}, \code{"-"}, \code{"*"}, \code{"/"}, \code{"^"}, \code{"\%\%"}, \code{"\%/\%"}
         	\item \code{"&"}, \code{"|"}, \code{"!"}
         	\item \code{"=="}, \code{"!="}, \code{"<"}, \code{"<="}, \code{">="}, \code{">"}
         	\item \code{min}, \code{max}, \code{sum}, \code{range}, \code{prod}
         }
         
         \item \code{\link[MonetDB.R]{sd}} -- Calculates the standard deviation (only for single columns)
         \item \code{\link[MonetDB.R]{var}} -- Calculates the variance (only for single columns)
         \item \code{\link[stats]{quantile}} -- Provides quantile values (only for single numeric columns)
         \item \code{\link[stats]{median}} -- Retrieves the Median value (only for single numeric columns)
         \item \code{\link[stats]{aggregate}} -- Computes summary statistics by groups
         \item \code{\link[MonetDB.R]{aggregatef}} -- Computes summary statistics by groups using formula
         \item \code{\link[=cbind]{rbind}} -- Combines \code{monet.frame} objects of compatible structure through concatenation
         \item \code{\link[base]{merge}} -- Merge two \code{monet.frame} objects together using a DB JOIN operation
         \item \code{\link[base]{sort}} -- Sort a database column ascending or descending (only for single columns)
         \item \code{\link[base]{tabulate}} -- Counts the number of value occurences (only for single numerical columns)
         \item \code{\link[base]{range}} -- Gives range of values (only for single numerical columns)
    
       }
}
\seealso{
  \code{\link[base]{data.frame}}
}
\examples{
\dontrun{
# connect to MonetDB
conn <- dbConnect(MonetDB.R(), "monetdb://localhost/demo")
# write some test data
data(iris)
dbWriteTable(conn, "iris", iris)
# construct monet.frame
frame <- monet.frame(conn,"iris")
# show description
str(frame)
# select one of the colums
frame$sepal_width
# select ten rows and the first two columns
frame[1:10,1:2]
# calculate aggregation
mean(frame$sepal_width)
}}
\keyword{interface}
