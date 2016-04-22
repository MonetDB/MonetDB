# C library that contains our MAPI string splitting state machine
C_LIBRARY <- "MonetDB.R"

# Make S4 aware of S3 classes
# setOldClass(c("sockconn", "connection"))

### MonetDBDriver
setClass("MonetDBDriver", representation("DBIDriver"))
setClass("MonetDBEmbeddedDriver", representation("MonetDBDriver"))

# allow instantiation of this driver with MonetDB to allow existing programs to work
MonetR <- MonetDB <- MonetDBR <- MonetDB.R <- function() new("MonetDBDriver")

MonetDBLite <- monetdblite <- RMonetDBLite <- rmonetdblite <- function() new("MonetDBEmbeddedDriver")

setMethod("dbIsValid", "MonetDBDriver", def=function(dbObj, ...) invisible(TRUE))

setMethod("dbUnloadDriver", "MonetDBDriver", def=function(drv, ...) invisible(TRUE))

setMethod("dbGetInfo", "MonetDBDriver", def=function(dbObj, ...)
  list(name="MonetDBDriver", 
       driver.version=utils::packageVersion("MonetDB.R"), 
       DBI.version=utils::packageVersion("DBI"), 
       client.version=NA, 
       max.connections=125) # R can only handle 128 connections, three of which are pre-allocated
)

# shorthand for connecting to the DB, very handy, e.g. dbListTables(mc("acs"))
mc <- function(dbname="demo", user="monetdb", password="monetdb", host="localhost", port=50000L, 
               timeout=86400L, wait=FALSE, language="sql", ...) {
  
  dbConnect(MonetDB.R(), dbname, user, password, host, port, timeout, wait, language, ...)
}

mq <- function(dbname, query, ...) {
  conn <- mc(dbname, ...)
  res <- dbGetQuery(conn, query)
  dbDisconnect(conn)
  return(res)
}

setMethod("dbConnect", "MonetDBDriver", def=function(drv, dbname="demo", user="monetdb", 
                                                     password="monetdb", host="localhost", port=50000L, timeout=86400L, wait=FALSE, language="sql", embedded=FALSE,
                                                     ..., url="") {
  
  if (substring(url, 1, 10) == "monetdb://" || substring(url, 1, 12) == "monetdblite:") {
    dbname <- url
  }
  timeout <- as.integer(timeout)
  
  if (substring(dbname, 1, 10) == "monetdb://") {
    rest <- substring(dbname, 11, nchar(dbname))
    # split at /, so we get the dbname
    slashsplit <- strsplit(rest, "/", fixed=TRUE)
    hostport <- slashsplit[[1]][1]
    dbname <- slashsplit[[1]][2]
    
    # count the number of : in the string
    ndc <- nchar(hostport) - nchar(gsub(":","",hostport,fixed=T))
    if (ndc == 0) {
      host <- hostport
    }
    if (ndc == 1) { # ipv4 case, any ipv6 address has more than one :
      hostportsplit <- strsplit(hostport, ":", fixed=TRUE)
      host <- hostportsplit[[1]][1]
      port <- hostportsplit[[1]][2]
    }
    if (ndc > 1) { # ipv6 case, now we only need to check for ]:
      if (length(grep("]:", hostport, fixed=TRUE)) == 1) { # ipv6 with port number
        hostportsplit <- strsplit(hostport, "]:", fixed=TRUE)
        host <- substring(hostportsplit[[1]][1],2)
        port <- hostportsplit[[1]][2]
      }
      else {
        host <- hostport
      }
    }
  }
  # this is important, otherwise we'll trip an assertion
  port <- as.integer(port)
  # validate port number
  if (length(port) != 1 || port < 1 || port > 65535) {
    stop("Illegal port number ",port)
  }

  # support monetdblite:/db/dir urls to fool sqlsurvey
  if (substring(dbname, 1, 12) == "monetdblite:") {
    embedded <- substring(dbname, 13, nchar(dbname))
  }

  if (inherits(drv, "MonetDBEmbeddedDriver")) {
    if (missing(dbname)) embedded <- tempdir()
    else embedded <- dbname
  }

  if (embedded != FALSE) {
    if (!requireNamespace("MonetDBLite", quietly=T)) {
      stop("MonetDBLite package required for embedded mode")
    }
    MonetDBLite::monetdb_embedded_startup(embedded, !getOption("monetdb.debug.embedded", FALSE), 
      getOption("monetdb.sequential", TRUE))
    connenv <- new.env(parent=emptyenv())
    connenv$conn <- MonetDBLite::monetdb_embedded_connect()
    connenv$open <- TRUE
    conn <- new("MonetDBEmbeddedConnection", connenv=connenv)
    attr(conn, "dbPreExists") <- TRUE
    return(conn)
  }
  
  if (getOption("monetdb.debug.mapi", F)) message("II: Connecting to MonetDB on host ", host, " at "
                                                  ,"port ", port, " to DB ", dbname, " with user ", user, " and a non-printed password, timeout is "
                                                  , timeout, " seconds.")
  socket <- FALSE
  if (wait) {
    repeat {
      continue <- FALSE
      tryCatch ({
        # open socket with 5-sec timeout so we can check whether everything works
        suppressWarnings(socket <- socket <<- .mapiConnect(host, port, 5))
        # authenticate
        .mapiAuthenticate(socket, dbname, user, password, language=language)
        .mapiDisconnect(socket)
        break
      }, error = function(e) {
        if ("connection" %in% class(socket)) {
          tryCatch(close(socket), error=function(e){}) 
        }
        message("Server not ready(", e$message, "), retrying (ESC or CTRL+C to abort)")
        Sys.sleep(1)
        continue <<- TRUE
      })
    }
  }
  
  # make new socket with user-specified timeout

  connenv <- new.env(parent=emptyenv())
  connenv$lock <- 0
  connenv$deferred <- list()
  connenv$exception <- list()
  connenv$params <- list(drv=drv, host=host, port=port, timeout=timeout, dbname=dbname, user=user, password=password, language=language)
  connenv$socket <- .mapiConnect(host, port, timeout) 
  .mapiAuthenticate(connenv$socket, dbname, user, password, language=language)
  
  conn <- new("MonetDBConnection", connenv=connenv)
  if (getOption("monetdb.sequential", F)) {
    message("MonetDB: Switching to single-threaded query execution.")
    dbSendQuery(conn, "set optimizer='sequential_pipe'")
  }
  attr(conn, "dbPreExists") <- TRUE
  conn
}, 
valueClass="MonetDBConnection")


### MonetDBConnection
setClass("MonetDBConnection", representation("DBIConnection", connenv="environment"))

setClass("MonetDBEmbeddedConnection", representation("MonetDBConnection", connenv="environment"))


setMethod("dbGetInfo", "MonetDBConnection", def=function(dbObj, ...) {
  envdata <- dbGetQuery(dbObj, "SELECT name, value from sys.env()")
  ll <- as.list(envdata$value)
  names(ll) <- envdata$name
  ll$name <- "MonetDBConnection"
  ll$db.version <- NA
  ll$dbname <- ll$gdk_dbname
  ll$username <- NA
  ll$host <- NA
  ll$port <- NA
  ll
})

setMethod("dbIsValid", "MonetDBConnection", def=function(dbObj, ...) {
  return(invisible(!is.na(tryCatch({dbGetInfo(dbObj);TRUE}, error=function(e){NA}))))
})

setMethod("dbDisconnect", "MonetDBConnection", def=function(conn, ...) {
  .mapiDisconnect(conn@connenv$socket)
  invisible(TRUE)
})

setMethod("dbDisconnect", "MonetDBEmbeddedConnection", def=function(conn, shutdown=FALSE, ...) {
  if (!conn@connenv$open) warning("already disconnected")
  conn@connenv$open <- FALSE
  MonetDBLite::monetdb_embedded_disconnect(conn@connenv$conn)
  if (shutdown) MonetDBLite::monetdb_embedded_shutdown()
  invisible(TRUE)
})

setMethod("dbListTables", "MonetDBConnection", def=function(conn, ..., sys_tables=F, schema_names=F) {
  q <- "select schemas.name as sn, tables.name as tn from sys.tables join sys.schemas on tables.schema_id=schemas.id"
  if (!sys_tables) q <- paste0(q, " where tables.system=false order by sn, tn")
  df <- dbGetQuery(conn, q)
  df$tn <- quoteIfNeeded(conn, df$tn, warn=F)
  res <- df$tn
  if (schema_names) {
    df$sn <- quoteIfNeeded(conn, df$sn, warn=F)
    res <- paste0(df$sn, ".", df$tn)
  }
  as.character(res)
})

if (is.null(getGeneric("dbTransaction"))) setGeneric("dbTransaction", function(conn, ...) 
  standardGeneric("dbTransaction"))

setMethod("dbTransaction", signature(conn="MonetDBConnection"),  def=function(conn, ...) {
  dbBegin(conn)
  warning("dbTransaction() is deprecated, use dbBegin() from now.")
  invisible(TRUE)
})

setMethod("dbBegin", "MonetDBConnection", def=function(conn, ...) {
  dbSendQuery(conn, "START TRANSACTION")
  invisible(TRUE)
})

setMethod("dbCommit", "MonetDBConnection", def=function(conn, ...) {
  dbSendQuery(conn, "COMMIT")
  invisible(TRUE)
})

setMethod("dbRollback", "MonetDBConnection", def=function(conn, ...) {
  dbSendQuery(conn, "ROLLBACK")
  invisible(TRUE)
})

setMethod("dbListFields", signature(conn="MonetDBConnection", name = "character"), def=function(conn, name, ...) {
  if (!dbExistsTable(conn, name))
    stop(paste0("Unknown table: ", name));
  df <- dbGetQuery(conn, paste0("select columns.name as name from sys.columns join sys.tables on \
    columns.table_id=tables.id where tables.name='", name, "';"))	
  df$name
})

setMethod("dbExistsTable", signature(conn="MonetDBConnection", name = "character"), def=function(conn, name, ...) {
  name <- quoteIfNeeded(conn, name)
  return(as.character(name) %in% 
    dbListTables(conn, sys_tables=T))
})

setMethod("dbGetException", "MonetDBConnection", def=function(conn, ...) {
  conn@connenv$exception
})

setMethod("dbReadTable", signature(conn="MonetDBConnection", name = "character"), def=function(conn, name, ...) {
  name <- quoteIfNeeded(conn, name)
  if (!dbExistsTable(conn, name))
    stop(paste0("Unknown table: ", name));
  dbGetQuery(conn, paste0("SELECT * FROM ", name), ...)
})

# This one does all the work in this class
setMethod("dbSendQuery", signature(conn="MonetDBConnection", statement="character"),  
          def=function(conn, statement, ..., list=NULL, async=FALSE) {   
  if(!is.null(list) || length(list(...))){
    if (length(list(...))) statement <- .bindParameters(statement, list(...))
    if (!is.null(list)) statement <- .bindParameters(statement, list)
  }	
  conn@connenv$exception <- list()
  env <- NULL
  if (getOption("monetdb.debug.query", F))  message("QQ: '", statement, "'")
  if(!is.null(log_file <- getOption("monetdb.log.query", NULL)))
    cat(c(statement, ";\n"), file = log_file, sep="", append = TRUE)
  # the actual request
  resp <- NA
  tryCatch({
    mresp <- .mapiRequest(conn, paste0("s", statement, "\n;"), async=async)
    resp <- .mapiParseResponse(mresp)
  }, interrupt = function(ex) {
    message("Interrupted query execution. Attempting to fix connection....")
      
    newconn <- do.call("dbConnect", conn@connenv$params)
    dbDisconnect(conn)
    conn@connenv$socket <- newconn@connenv$socket
    conn@connenv$lock <- 0
    conn@connenv$deferred <- list()
    conn@connenv$exception <- list()

    stop("No query result for now.")
  })

  env <- new.env(parent=emptyenv())

  if (resp$type == Q_TABLE) {
    # we have to pass this as an environment to make conn object available to result for fetching
    env$success = TRUE
    env$conn <- conn
    env$data <- resp$tuples
    resp$tuples <- NULL # clean up
    env$info <- resp
    env$delivered <- -1
    env$query <- statement
    env$open <- TRUE
  }
  if (resp$type == Q_UPDATE || resp$type == Q_CREATE || resp$type == MSG_ASYNC_REPLY || resp$type == MSG_PROMPT) {
    env$success = TRUE
    env$conn <- conn
    env$query <- statement
    env$info <- resp

  }
  if (resp$type == MSG_MESSAGE) {
    env$success = FALSE
    env$conn <- conn
    env$query <- statement
    env$info <- resp
    env$message <- resp$message
  }

  if (!env$success) {
    sp <- strsplit(env$message, "!", fixed=T)[[1]]
    # truncate statement to not hide actual error message
    if (nchar(statement) > 100) { statement <- paste0(substring(statement, 1, 100), "...") }
    if (length(sp) == 3) {
      errno <- sp[[2]]
      errmsg <- sp[[3]]
      conn@connenv$exception <- list(errNum=errno, errMsg=errmsg)
      stop("Unable to execute statement '", statement, "'.\nServer says '", errmsg, "' [#", 
           errno, "].")
    }
    else {
      conn@connenv$exception <- list(errNum=NA, errMsg=env$message)
      stop("Unable to execute statement '", statement, "'.\nServer says '", env$message, "'.")
    }
  }

  invisible(new("MonetDBResult", env=env))
  })


# This one does all the work in this class
setMethod("dbSendQuery", signature(conn="MonetDBEmbeddedConnection", statement="character"),  
          def=function(conn, statement, ..., list=NULL, execute = T, resultconvert = T) {   
  if (!conn@connenv$open) {
    stop("This connection was closed.")
  }
  if(!is.null(list) || length(list(...))){
    if (length(list(...))) statement <- .bindParameters(statement, list(...))
    if (!is.null(list)) statement <- .bindParameters(statement, list)
  } 
  env <- NULL
  if (getOption("monetdb.debug.query", F)) message("QQ: '", statement, "'")
  if(!is.null(log_file <- getOption("monetdb.log.query", NULL)))
    cat(c(statement, ";\n"), file = log_file, sep="", append = TRUE)
  startt <- Sys.time()
  resp <- MonetDBLite::monetdb_embedded_query(conn@connenv$conn, statement, execute, resultconvert)
  takent <- round(as.numeric(Sys.time() - startt), 2)
  env <- new.env(parent=emptyenv())
  if (resp$type == Q_TABLE) {
    meta <- new.env(parent=emptyenv())
    meta$type  <- Q_TABLE
    meta$id    <- -1
    meta$rows  <- NROW(resp$tuples)
    meta$cols  <- NCOL(resp$tuples)
    meta$index <- 0
    meta$names <- names(resp$tuples)

    env$info <- meta
    env$success = TRUE
    env$conn <- conn
    env$resp <- resp
    env$delivered <- -1
    env$query <- statement
    env$open <- TRUE
  }
  if (resp$type == Q_UPDATE || resp$type == Q_CREATE || resp$type == MSG_ASYNC_REPLY || resp$type == MSG_PROMPT) {
    env$success = TRUE
    env$conn <- conn
    env$query <- statement
    env$info <- resp
    env$info$rows <- 0
  }
  if (resp$type == MSG_MESSAGE) {
    env$success = FALSE
    env$conn <- conn
    env$query <- statement
    env$info <- resp
    env$message <- resp$message
  }

  if (!env$success) {
    sp <- strsplit(env$message, "!", fixed=T)[[1]]
    # truncate statement to not hide actual error message
    if (nchar(statement) > 100) { statement <- paste0(substring(statement, 1, 100), "...") }
    if (length(sp) == 3) {
      errno <- sp[[2]]
      errmsg <- sp[[3]]
      conn@connenv$exception <- list(errNum=errno, errMsg=errmsg)
      stop("Unable to execute statement '", statement, "'.\nServer says '", errmsg, "' [#", 
           errno, "].")
    }
    else {
      conn@connenv$exception <- list(errNum=NA, errMsg=env$message)
      stop("Unable to execute statement '", statement, "'.\nServer says '", env$message, "'.")
    }
  }
  if (getOption("monetdb.debug.query", F)) message("II: Finished in ", takent, "s")

  invisible(new("MonetDBEmbeddedResult", env=env))
  })

# found from sql_scan.c and trial/error
reserved_monetdb_keywords <- sort(unique(toupper(c(.SQL92Keywords, 
"ADD", "ADMIN", "AFTER", "AGGREGATE", "ALL", "ALTER", "ALWAYS", 
"AND", "ANY", "ASC", "ASYMMETRIC", "ATOMIC", "AUTO_INCREMENT", 
"BEFORE", "BEGIN", "BEST", "BETWEEN", "BIGINT", "BIGSERIAL", 
"BINARY", "BLOB", "BY", "CALL", "CASCADE", "CASE", "CAST", "CHAIN", 
"CHAR", "CHARACTER", "CHECK", "CLOB", "COALESCE", "COMMIT", "COMMITTED", 
"CONSTRAINT", "CONVERT", "COPY", "CORRESPONDING", "CREATE", "CROSS", 
"CURRENT", "CURRENT_DATE", "CURRENT_ROLE", "CURRENT_TIME", "CURRENT_TIMESTAMP", 
"CURRENT_USER", "DAY", "DEC", "DECIMAL", "DECLARE", "DEFAULT", 
"DELETE", "DELIMITERS", "DESC", "DO", "DOUBLE", "DROP", "EACH", 
"EFFORT", "ELSE", "ELSEIF", "ENCRYPTED", "END", "ESCAPE", "EVERY", 
"EXCEPT", "EXCLUDE", "EXISTS", "EXTERNAL", "EXTRACT", "FALSE", 
"FLOAT", "FOLLOWING", "FOR", "FOREIGN", "FROM", "FULL", "FUNCTION", 
"GENERATED", "GLOBAL", "GRANT", "GROUP", "HAVING", "HOUR", "HUGEINT", 
"IDENTITY", "IF", "ILIKE", "IN", "INDEX", "INNER", "INSERT", 
"INT", "INTEGER", "INTERSECT", "INTO", "IS", "ISOLATION", "JOIN", 
"LEFT", "LIKE", "LIMIT", "LOCAL", "LOCALTIME", "LOCALTIMESTAMP", 
"LOCKED", "MEDIUMINT", "MERGE", "MINUTE", "MONTH", "NATURAL", 
"NEW", "NEXT", "NOCYCLE", "NOMAXVALUE", "NOMINVALUE", "NOT", 
"NOW", "NULL", "NULLIF", "NUMERIC", "OF", "OFFSET", "OLD", "ON", 
"ONLY", "OPTION", "OR", "ORDER", "OTHERS", "OUTER", "OVER", "PARTIAL", 
"PARTITION", "POSITION", "PRECEDING", "PRESERVE", "PRIMARY", 
"PRIVILEGES", "PROCEDURE", "PUBLIC", "RANGE", "READ", "REAL", 
"RECORDS", "REFERENCES", "REFERENCING", "REMOTE", "RENAME", "REPEATABLE", 
"REPLICA", "RESTART", "RESTRICT", "RETURN", "RETURNS", "REVOKE", 
"RIGHT", "ROLLBACK", "ROWS", "SAMPLE", "SAVEPOINT", "SECOND", 
"SELECT", "SEQUENCE", "SERIAL", "SERIALIZABLE", "SESSION_USER", 
"SET", "SIMPLE", "SMALLINT", "SOME", "SPLIT_PART", "STDIN", "STDOUT", 
"STORAGE", "STREAM", "STRING", "SUBSTRING", "SYMMETRIC", "THEN", 
"TIES", "TINYINT", "TO", "TRANSACTION", "TRIGGER", "TRUE", "UNBOUNDED", 
"UNCOMMITTED", "UNENCRYPTED", "UNION", "UNIQUE", "UPDATE", "USER", 
"USING", "VALUES", "VARCHAR", "VARYING", "VIEW", "WHEN", "WHERE", 
"WHILE", "WITH", "WORK", "WRITE", "XMLAGG", "XMLATTRIBUTES", 
"XMLCOMMENT", "XMLCONCAT", "XMLDOCUMENT", "XMLELEMENT", "XMLFOREST", 
"XMLNAMESPACES", "XMLPARSE", "XMLPI", "XMLQUERY", "XMLSCHEMA", 
"XMLTEXT", "XMLVALIDATE", "YEAR"))))

# quoting
quoteIfNeeded <- function(conn, x, warn=T, ...) {
  x <- as.character(x)
  chars <- !grepl("^[a-z_][a-z0-9_]*$", x, perl=T) & !grepl("^\"[^\"]*\"$", x, perl=T)
  if (any(chars) && warn) {
    message("Identifier(s) ", paste("\"", x[chars],"\"", collapse=", ", sep=""), " contain uppercase or reserved SQL characters and need(s) to be quoted in queries.")
  }
  reserved <- toupper(x) %in% reserved_monetdb_keywords
  if (any(reserved) && warn) {
    message("Identifier(s) ", paste("\"", x[reserved],"\"", collapse=", ", sep=""), " are reserved SQL keywords and need(s) to be quoted in queries.")
  }
  qts <- reserved | chars
  if (any(qts)) {
    x[qts] <- dbQuoteIdentifier(conn, x[qts])
  }
  x
}

setMethod("dbWriteTable", signature(conn="MonetDBConnection", name = "character", value="ANY"), def=function(conn, name, value, overwrite=FALSE, 
  append=FALSE, csvdump=FALSE, transaction=TRUE, temporary=FALSE, ...) {
  if (is.character(value)) {
    message("Treating character vector parameter as file name(s) for monetdb.read.csv()")
    monetdb.read.csv(conn=conn, files=value, tablename=name, create=!append, ...)
    return(invisible(TRUE))
  }
  if (is.vector(value) && !is.list(value)) value <- data.frame(x=value, stringsAsFactors=F)
  if (length(value) < 1) stop("value must have at least one column")
  if (is.null(names(value))) names(value) <- paste("V", 1:length(value), sep='')
  if (length(value[[1]])>0) {
    if (!is.data.frame(value)) value <- as.data.frame(value, row.names=1:length(value[[1]]) , stringsAsFactors=F)
  } else {
    if (!is.data.frame(value)) value <- as.data.frame(value, stringsAsFactors=F)
  }
  if (overwrite && append) {
    stop("Setting both overwrite and append to TRUE makes no sense.")
  }
  if (transaction) {
    dbBegin(conn)
    on.exit(tryCatch(dbRollback(conn), error=function(e){}))
  }
  qname <- quoteIfNeeded(conn, name)
  if (dbExistsTable(conn, qname)) {
    if (overwrite) dbRemoveTable(conn, qname)
    if (!overwrite && !append) stop("Table ", qname, " already exists. Set overwrite=TRUE if you want 
      to remove the existing table. Set append=TRUE if you would like to add the new data to the 
      existing table.")
  }
  
  if (!dbExistsTable(conn, qname)) {
    fts <- sapply(value, dbDataType, dbObj=conn)
    fdef <- paste(quoteIfNeeded(conn, names(value)), fts, collapse=', ')
    if (temporary) {
      ct <- paste0("CREATE TEMPORARY TABLE ", qname, " (", fdef, ") ON COMMIT PRESERVE ROWS")
    } else {
      ct <- paste0("CREATE TABLE ", qname, " (", fdef, ")")
    }
    dbSendUpdate(conn, ct)
  }
  if (length(value[[1]])) {
    classes <- unlist(lapply(value, class))
    for (c in names(classes[classes=="character"])) {
      value[[c]] <- enc2utf8(value[[c]])
    }
    for (c in names(classes[classes=="factor"])) {
      levels(value[[c]]) <- enc2utf8(levels(value[[c]]))
    }
    if (inherits(conn, "MonetDBEmbeddedConnection")) {
      if (csvdump) {
        warning("Ignoring csvdump setting in embedded mode")
      }
      # convert Date cols to characters
      # TODO: use type mapping to select correct converters
      for (c in names(classes[classes=="Date"])) {
        value[[c]] <- as.character(value[[c]])
      }

      insres <- MonetDBLite::monetdb_embedded_append(conn@connenv$conn, qname, value)
      if (!is.logical(insres)) {
        stop("Failed to insert data: ", insres)
      }
    }
    else {
      if (csvdump) {
        tmp <- tempfile(fileext = ".csv")
        write.table(value, tmp, sep = ",", quote = TRUE, row.names = FALSE, col.names = FALSE, na="", fileEncoding = "UTF-8")
        dbSendQuery(conn, paste0("COPY INTO ", qname, " FROM '", tmp, "' USING DELIMITERS ',','\\n','\"' NULL AS ''"))
        file.remove(tmp) 
      } else {
        vins <- paste("(", paste(rep("?", length(value)), collapse=', '), ")", sep='')
        # chunk some inserts together so we do not need to do a round trip for every one
        splitlen <- 0:(nrow(value)-1) %/% getOption("monetdb.insert.splitsize", 1000)
        lapply(split(value, splitlen), 
          function(valueck) {
          bvins <- c()
          for (j in 1:length(valueck[[1]])) bvins <- c(bvins,.bindParameters(vins, as.list(valueck[j, ])))
          dbSendUpdate(conn, paste0("INSERT INTO ", qname, " VALUES ",paste0(bvins, collapse=", ")))
        })
      }
    }
  }
  if (transaction) {
    dbCommit(conn)
    on.exit(NULL)
  }
  return(invisible(TRUE))
})

setMethod("dbDataType", signature(dbObj="MonetDBConnection", obj = "ANY"), def = function(dbObj, 
                                                                                          obj, ...) {
  if (is.logical(obj)) "BOOLEAN"
  else if (is.integer(obj)) "INTEGER"
  else if (is.numeric(obj)) "DOUBLE PRECISION"
  else if (is.raw(obj)) "BLOB"
  else "STRING"
}, valueClass = "character")


setMethod("dbRemoveTable", signature(conn="MonetDBConnection", name = "character"), def=function(conn, name, ...) {
  name <- quoteIfNeeded(conn, name)
  if (dbExistsTable(conn, name)) {
    dbSendUpdate(conn, paste("DROP TABLE", name))
    return(invisible(TRUE))
  }
  return(invisible(FALSE))
})

# for compatibility with RMonetDB (and dbWriteTable support), we will allow parameters to this 
# method, but will not use prepared statements internally
if (is.null(getGeneric("dbSendUpdate"))) setGeneric("dbSendUpdate", function(conn, statement, ..., 
                                                                             async=FALSE) standardGeneric("dbSendUpdate"))
setMethod("dbSendUpdate", signature(conn="MonetDBConnection", statement="character"),  
          def=function(conn, statement, ..., list=NULL, async=FALSE) {
            
            if(!is.null(list) || length(list(...))){
              if (length(list(...))) statement <- .bindParameters(statement, list(...))
              if (!is.null(list)) statement <- .bindParameters(statement, list)
            }
            res <- dbSendQuery(conn, statement, async=async)
            if (!res@env$success) {
              stop(paste(statement, "failed!\nServer says:", res@env$message))
            }
            return(invisible(TRUE))
          })

# this can be used in finalizers to not mess up the socket
if (is.null(getGeneric("dbSendUpdateAsync"))) setGeneric("dbSendUpdateAsync", function(conn, 
                                                                                       statement, ...) standardGeneric("dbSendUpdateAsync"))
setMethod("dbSendUpdateAsync", signature(conn="MonetDBConnection", statement="character"),  
          def=function(conn, statement, ..., list=NULL) {
            
            dbSendUpdate(conn, statement, async=TRUE)
          })



# mapiQuote(toString(value))

.bindParameters <- function(statement, param) {
  for (i in 1:length(param)) {
    value <- param[[i]]
    valueClass <- class(value)
    if (is.na(value)) 
      statement <- sub("?", "NULL", statement, fixed=TRUE)
    else if (valueClass %in% c("numeric", "logical", "integer"))
      statement <- sub("?", value, statement, fixed=TRUE)
    else if (valueClass == c("raw"))
      stop("raw() data is so far only supported when reading from BLOBs")
    else
      statement <- sub("?", paste("'", .mapiQuote(toString(value)), "'", sep=""), statement, 
                       fixed=TRUE)
  }
  return(statement)
}

# quote strings when sending them to the db. single quotes are most critical.
# null bytes are not supported
.mapiQuote <- function(str) {
  qs <- ""
  chrs <- unlist(strsplit(str, "", fixed=TRUE))
  for (chr in chrs) {
    f <- ""
    if (chr == "\n") f <- qs <- paste0(qs, "\\", "n")
    if (chr == "\t") f <- qs <- paste0(qs, "\\", "t")
    if (chr == "'" ) f <- qs <- paste0(qs, "\\'")
    if (nchar(f) == 0) qs <- paste0(qs, chr)
  }
  qs
}


### MonetDBResult
setClass("MonetDBResult", representation("DBIResult", env="environment"))
setClass("MonetDBEmbeddedResult", representation("MonetDBResult", env="environment"))


.CT_INT <- 0L
.CT_NUM <- 1L
.CT_CHR <- 2L
.CT_BOOL <- 3L
.CT_RAW <- 4L

# type mapping matrix
monetTypes <- rep(c("integer", "numeric", "character", "character", "logical", "raw"), c(5, 6, 4, 6, 1, 1))
names(monetTypes) <- c(c("WRD", "TINYINT", "SMALLINT", "INT", "MONTH_INTERVAL"), # month_interval is the diff between date cols, int
  c("BIGINT", "HUGEINT", "REAL", "DOUBLE", "DECIMAL", "SEC_INTERVAL"),  # sec_interval is the difference between timestamps, float
  c("CHAR", "VARCHAR", "CLOB", "STR"), 
  c("INTERVAL", "DATE", "TIME", "TIMETZ", "TIMESTAMP", "TIMESTAMPTZ"), 
  c("BOOLEAN"), 
  c("BLOB"))

monetdbRtype <- function(dbType) {
  dbType <- toupper(dbType)
  rtype <- monetTypes[dbType]
  if (is.na(rtype)) {
    stop("Unknown DB type ", dbType)
  }
  rtype
}

setMethod("fetch", signature(res="MonetDBResult", n="numeric"), def=function(res, n, ...) {
  # DBI on CRAN still uses fetch()
  # .Deprecated("dbFetch")
  dbFetch(res, n, ...)
})

# most of the heavy lifting here
setMethod("dbFetch", signature(res="MonetDBResult", n="numeric"), def=function(res, n, ...) {
  if (!res@env$success) {
    stop("Cannot fetch results from error response, error was ", res@env$info$message)
  }
  if (!dbIsValid(res)) {
    stop("Cannot fetch results from closed response.")
  }
 
  # okay, so we arrive here with the tuples from the first result in res@env$data as a list
  info <- res@env$info
  # apparently, one should be able to fetch results sets from ddl ops
  if (info$type == Q_UPDATE) { 
    return(data.frame())
  }
  if (res@env$delivered < 0) {
    res@env$delivered <- 0
  }
  stopifnot(res@env$delivered <= info$rows, info$index <= info$rows)
  remaining <- info$rows - res@env$delivered
    
  if (n < 0) {
    n <- remaining
  } else {
    n <- min(n, remaining)
  }  
  
  # prepare the result holder df with columns of the appropriate type
  df = list()
  ct <- rep(0L, info$cols)
  
  for (i in seq.int(info$cols)) {
    rtype <- monetdbRtype(info$types[i])
    if (rtype=="integer") {      
      df[[i]] <- integer()
      ct[i] <- .CT_INT
    }
    if (rtype=="numeric") {			
      df[[i]] <- numeric()
      ct[i] <- .CT_NUM
    }
    if (rtype=="character") {
      df[[i]] <- character()
      ct[i] <- .CT_CHR			
    }
    if (rtype=="logical") {
      df[[i]] <- logical()
      ct[i] <- .CT_BOOL			
    }
    if (rtype=="raw") {
      df[[i]] <- raw()
      ct[i] <- .CT_RAW
    }
    names(df)[i] <- info$names[i]
  }
  
  # we have delivered everything, return empty df (spec is not clear on this one...)
  if (n < 1) {
    return(data.frame(df, stringsAsFactors=F))
  }
  
  # if our tuple cache in res@env$data does not contain n rows, we fetch from server until it does
  while (length(res@env$data) < n) {
    cresp <- .mapiParseResponse(.mapiRequest(res@env$conn, paste0("Xexport ", .mapiLongInt(info$id), 
      " ", .mapiLongInt(info$index), " ", .mapiLongInt(n-length(res@env$data)))))
    stopifnot(cresp$type == Q_BLOCK && cresp$rows > 0)
    
    res@env$data <- c(res@env$data, cresp$tuples)
    info$index <- info$index + cresp$rows
    # if (getOption("monetdb.profile", T))  .profiler_progress(length(res@env$data), n)
  }
  
  # convert tuple string vector into matrix so we can access a single column efficiently
  # call to a faster C implementation for the annoying task of splitting everyting into fields
  parts <- .Call("mapi_split", res@env$data[1:n], as.integer(info$cols), PACKAGE=C_LIBRARY)
  
  # convert values column by column
  for (j in seq.int(info$cols)) {	
    col <- ct[[j]]
    if (col == .CT_INT) 
      df[[j]] <- as.integer(parts[[j]])
    if (col == .CT_NUM) 
      df[[j]] <- as.numeric(parts[[j]])
    if (col == .CT_BOOL) 
      df[[j]] <- parts[[j]]=="true"
    if (col == .CT_CHR) { 
      df[[j]] <- parts[[j]]
      Encoding(df[[j]]) <- "UTF-8"
    }
    if (col == .CT_RAW) {
      df[[j]] <- lapply(parts[[j]], charToRaw)
    }
  }
  
  # remove the already delivered tuples from the background holder or clear it altogether
  if (n+1 >= length(res@env$data)) {
    res@env$data <- character()
  }
  else {
    res@env$data <- res@env$data[seq(n+1, length(res@env$data))]
  }
  res@env$delivered <- res@env$delivered + n
  
  # this is a trick so we do not have to call data.frame(), which is expensive
  attr(df, "row.names") <- c(NA_integer_, length(df[[1]]))
  class(df) <- "data.frame"
  
  # if (getOption("monetdb.profile", T))  .profiler_clear()
  df
})

# most of the heavy lifting here
setMethod("dbFetch", signature(res="MonetDBEmbeddedResult", n="numeric"), def=function(res, n, ...) {
  if (!res@env$success) {
    stop("Cannot fetch results from error response, error was ", res@env$info$message)
  }
  if (!dbIsValid(res)) {
    stop("Cannot fetch results from closed response.")
  }
  if (n == 0) {
    stop("Fetch 0 rows? Really?")
  }
  if (res@env$info$type == Q_UPDATE) { 
    return(data.frame())
  }
  if (res@env$delivered < 0) {
    res@env$delivered <- 0
  }
  if (res@env$delivered >= res@env$info$rows) {
    return(res@env$resp$tuples[F,, drop=F])
  }
  if (n > -1) {
    n <- min(n, res@env$info$rows - res@env$delivered)
    res@env$delivered <- res@env$delivered + n
    return(res@env$resp$tuples[(res@env$delivered - n + 1):(res@env$delivered),, drop=F])
  }
  else {
    res@env$delivered <- res@env$info$rows
    return(res@env$resp$tuples)
  }
})

setMethod("dbClearResult", "MonetDBResult", def = function(res, ...) {
  if (res@env$info$type == Q_TABLE) {
    resid <- res@env$info$id
    if (!is.null(resid) && !is.na(resid) && is.numeric(resid)) {
      .mapiRequest(res@env$conn, paste0("Xclose ", resid), async=TRUE)
      res@env$open <- FALSE
    }
  }
  return(invisible(TRUE))
}, valueClass = "logical")

setMethod("dbClearResult", "MonetDBEmbeddedResult", def = function(res, ...) {
  if (res@env$info$type == Q_TABLE) {
    res@env$open <- FALSE
  }
  return(invisible(TRUE))
}, valueClass = "logical")

setMethod("dbHasCompleted", "MonetDBResult", def = function(res, ...) {
  if (res@env$info$type == Q_TABLE) {
    return(res@env$delivered == res@env$info$rows)
  }
  return(invisible(TRUE))
}, valueClass = "logical")

# compatibility with RSQLite
if (is.null(getGeneric("isIdCurrent"))) setGeneric("isIdCurrent", function(dbObj, ...) standardGeneric("isIdCurrent"))
setMethod("isIdCurrent", signature(dbObj="MonetDBResult"), def=function(dbObj, ...) {
  .Deprecated("dbIsValid")
   dbIsValid(dbObj)
})

setMethod("isIdCurrent", signature(dbObj="MonetDBConnection"), def=function(dbObj, ...) {
  .Deprecated("dbIsValid")
   dbIsValid(dbObj)
})

if (is.null(getGeneric("initExtension"))) setGeneric("initExtension", function(dbObj, ...) standardGeneric("initExtension"))
setMethod("initExtension", signature(dbObj="MonetDBConnection"), def=function(dbObj, ...) {
  .Deprecated(msg="initExtension() is not required for MonetDBLite")
})


setMethod("dbIsValid", signature(dbObj="MonetDBResult"), def=function(dbObj, ...) {
  if (dbObj@env$info$type == Q_TABLE) {
    return(dbObj@env$open)
  }
  return(invisible(TRUE))
})

setMethod("dbColumnInfo", "MonetDBResult", def = function(res, ...) {
  data.frame(field.name=res@env$info$names, field.type=res@env$info$types, 
                    data.type=monetTypes[res@env$info$types], r.data.type=monetTypes[res@env$info$types], 
                    monetdb.data.type=res@env$info$types, stringsAsFactors=F)	
}, 
valueClass = "data.frame")

setMethod("dbColumnInfo", "MonetDBEmbeddedResult", def = function(res, ...) {
  data.frame(field.name=res@env$info$names, stringsAsFactors=F)  
  # TODO: also export SQL types? Do we need this?
}, 
valueClass = "data.frame")

setMethod("dbGetStatement", "MonetDBResult", def = function(res, ...) {
  res@env$query
}, 
valueClass = "character")

setMethod("dbGetRowCount", "MonetDBResult", def = function(res, ...) {
  res@env$info$rows
}, 
valueClass = "numeric")

setMethod("dbGetRowsAffected", "MonetDBResult", def = function(res, ...) {
  as.numeric(NA)
}, 
valueClass = "numeric")

# adapted from RMonetDB, no java-specific things in here...
monet.read.csv <- monetdb.read.csv <- function(conn, files, tablename, header=TRUE, 
                                               locked=FALSE, best.effort=FALSE, na.strings="", nrow.check=500, 
                                               delim=",", newline="\\n", quote="\"", create=TRUE, 
                                               col.names=NULL, lower.case.names=FALSE, sep=delim, ...){
  
  if (length(na.strings)>1) stop("na.strings must be of length 1")
  if (!missing(sep)) delim <- sep

  headers <- lapply(files, utils::read.csv, sep=delim, na.strings=na.strings, quote=quote, nrows=nrow.check, header=header, ...)
  if (length(files)>1){
    nn <- sapply(headers, ncol)
    if (!all(nn==nn[1])) stop("Files have different numbers of columns")
    nms <- sapply(headers, names)
    if(!all(nms==nms[, 1])) stop("Files have different variable names")
    types <- sapply(headers, function(df) sapply(df, dbDataType, dbObj=conn))
    if(!all(types==types[, 1])) stop("Files have different variable types")
  } 
  dbBegin(conn)
  on.exit(tryCatch(dbRollback(conn), error=function(e){}))
  if (create) {
  tablename <- quoteIfNeeded(conn, tablename)
    if(lower.case.names) names(headers[[1]]) <- tolower(names(headers[[1]]))
    if(!is.null(col.names)) {
      if (lower.case.names) {
        warning("Ignoring lower.case.names parameter as overriding col.names are supplied.")
      }
      col.names <- as.character(col.names)
      if (length(unique(col.names)) != length(names(headers[[1]]))) {
        stop("You supplied ", length(unique(col.names)), " unique column names, but file has ", 
          length(names(headers[[1]])), " columns.")
      }
      names(headers[[1]]) <- quoteIfNeeded(conn, col.names)
    }
    dbWriteTable(conn, tablename, headers[[1]][FALSE, ], transaction=F)
  }
  
  delimspec <- paste0("USING DELIMITERS '", delim, "','", newline, "','", quote, "'")
  
  for(i in seq_along(files)) {
    thefile <- normalizePath(files[i])
    dbSendUpdate(conn, paste("COPY", if(header) "OFFSET 2", "INTO", 
      tablename, "FROM", paste("'", thefile, "'", sep=""), delimspec, "NULL as", paste("'", 
      na.strings[1], "'", sep=""), if(locked) "LOCKED", if(best.effort) "BEST EFFORT"))
  }
  dbGetQuery(conn, paste("SELECT COUNT(*) FROM", tablename))[[1]]
  dbCommit(conn)
  on.exit(NULL)
}

