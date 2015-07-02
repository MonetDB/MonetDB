# TOOD: support running this on a select in addition to table?
# TODO: support remote dbs, find out whether its local via canary file
# TODO: don't actually construct the data frame but use attr/class trick to save copying

if (is.null(getGeneric("mdbapply"))) setGeneric("mdbapply", function(conn, table, fun, ...) 
  standardGeneric("mdbapply"))

setMethod("mdbapply", signature(conn="MonetDBConnection"),  def=function(conn, table, fun, ...) {
  # make sure table exists
  if (!dbExistsTable(conn, table)) {
    stop("Table ", table, " does not exist.")
  }

  # generate unique function name
  dbfunname <- "mdbapply_autogen_"
  while (dbGetQuery(conn, paste0("select count(*) from functions where name='", dbfunname, "'"))[[1]] > 0)
    dbfunname <- paste0(dbfunname, sample(letters, 1))

  # test R integration with dummy function
  dbBegin(conn)
  dbSendQuery(conn,paste0("CREATE FUNCTION ", dbfunname, "() RETURNS TABLE(d INTEGER) LANGUAGE R {1L}"))
  res <- dbGetQuery(conn,paste0("SELECT * FROM ", dbfunname, "()"))[[1]]
  dbRollback(conn)

  # now generate the UDF
  # find packages loaded here and load them on the server as well
  toloadpkgs <- setdiff(unique(sapply(strsplit(grep("^package:",search(),value=T),":"),function(x) x[[2]])),c("base","stats","methods","utils","codetools","graphics","grDevices","datasets","MonetDB.R","DBI","digest"))
  dbrcode <- ''
  if (length(toloadpkgs) > 0) {
    if (getOption("monetdb.debug.query",FALSE)) 
      message("Package(s)  ",paste0(toloadpkgs,collapse=", "))
    dbrcode <- paste0('# loading packages\ninvisible(lapply(setdiff(',
                      paste0(deparse(toloadpkgs),collapse=""),
                      ',unique(sapply(strsplit(grep("^package:", search(), value=T),":"), function(x) x[[2]]))), function(pname) library(pname, character.only=T, quietly=T)))\n')
  }
  # serialize global variables into ascii string, and add the code to scan it again into the current env
  vars <- codetools::findGlobals(fun, merge=F)$variables
  mdbapply_dotdot <- list(...)
  if (length(mdbapply_dotdot) > 0) {
    vars <- c(vars,"mdbapply_dotdot")
    assign("mdbapply_dotdot", mdbapply_dotdot, envir=environment(fun))
  }
  sfilename <- NA
  if (length(vars) > 0) {
    if (getOption("monetdb.debug.query",FALSE)) 
      message("Variable(s) ",paste0(vars,collapse=", "))
    sfilename <- tempfile()
    save(list=vars,file=sfilename,envir=environment(fun), compress=T)
    dbrcode <- paste0(dbrcode, '# load serialized global variable(s) ', paste(vars, collapse=", "), '\nload("', sfilename, '")\n')
  }

  rfilename <- tempfile()
  # get source of user function and append
  dbrcode <- paste0(dbrcode, "# user-supplied function\nmdbapply_userfun <- ", paste0(deparse(fun), collapse="\n"), 
    "\n# calling user function\nsaveRDS(do.call(mdbapply_userfun, if(exists('mdbapply_dotdot')){c(list(mdbapply_dbdata), mdbapply_dotdot)} else{list(mdbapply_dbdata)}),file=\"", rfilename, "\")\nreturn(42L)\n")
  
  # find out things about the table, then wrap the R function
  query <- paste0("SELECT * FROM ", table, " AS t")
  res <- monetdb_queryinfo(conn, query)
  dbfun <- paste0("CREATE FUNCTION ", dbfunname,"(", paste0(dbQuoteIdentifier(conn, res$names)," ", res$dbtypes, collapse=", "),
                  ") \nRETURNS TABLE(retval INTEGER) LANGUAGE R {\n# rename arguments\nmdbapply_dbdata <- data.frame(",
                  paste0(res$names, collapse=", "),", stringsAsFactors=F)\n", dbrcode, "};\n")
  # call the function we just created
  dbsel <- paste0("SELECT * FROM ", dbfunname, "( (",query,") );\n")
  # ok, talk to DB (easiest part of this)
  res <- NA
  dbBegin(conn)
  tryCatch({
    dbSendQuery(conn, dbfun)
    dbGetQuery(conn, dbsel)
    res <- readRDS(rfilename)
  }, finally={
    dbRollback(conn)
    file.remove(stats::na.omit(c(sfilename, rfilename)))
  })
  res
})


