.encodeGlobals <- function(name) {
  vars <- findGlobals(name,merge=F)$variables
  if (length(vars) < 1) {
    return(NA)
  }
  if (getOption("monetdb.debug.query",FALSE)) 
    message("Variable(s) ",paste0(vars,collapse=", "))

    # TODO: optionally inline serialized context for remote dbs
  res <- tempfile()
  save(list=vars,file=res,envir=environment(name),compress=T)
  return(res)
}

if (is.null(getGeneric("dbApply"))) setGeneric("dbApply", function(conn, ...) 
  standardGeneric("dbApply"))

setMethod("dbApply", signature(conn="MonetDBConnection"),  def=function(conn, table, rettype, fun) {
  # generate unique function name
  dbfunname <- "__r_dapply_autogen_"
  while (dbGetQuery(conn,paste0("select count(*) from functions where name='",dbfunname,"'"))[[1]] > 0)
    dbfunname <- paste0(dbfunname,sample(letters,1))

  # test R integration with dummy function
  dbBegin(conn)
  dbSendQuery(conn,paste0("CREATE FUNCTION ",dbfunname,"() RETURNS TABLE(d INTEGER) LANGUAGE R {1L}"))
  res <- dbGetQuery(conn,paste0("SELECT * FROM ",dbfunname,"()"))[[1]]
  dbRollback(conn)

  # now generate the UDF
  # find packages loaded here and load them on the server as well
  toloadpkgs <- setdiff(unique(sapply(strsplit(grep("^package:",search(),value=T),":"),function(x) x[[2]])),c("base","stats","methods","utils","codetools","graphics","grDevices","datasets"))
  dbrcode <- ''
  if (length(toloadpkgs) > 0) {
    if (getOption("monetdb.debug.query",FALSE)) 
      message("Package(s)  ",paste0(toloadpkgs,collapse=", "))
    dbrcode <- paste0('# loading packages\ninvisible(lapply(setdiff(',
                      paste0(deparse(toloadpkgs),collapse=""),
                      ',unique(sapply(strsplit(grep("^package:", search(), value=T),":"), function(x) x[[2]]))), function(pname) library(pname, character.only=T, quietly=T)))\n')
  }
  # serialize global variables into ascii string, and add the code to scan it again into the current env
  sfilename <- .encodeGlobals(fun)
  if (!is.na(sfilename)) {
    dbrcode <- paste0(dbrcode,'# load serialized global variables\nload("',sfilename,'")\n')
  }
  # get source of user function and append
  dbrcode <- paste0(dbrcode,"# user-supplied function\n.userfun <- ",paste0(deparse(fun),collapse="\n"),"\n# calling user function\nreturn(.userfun(.dbdata))\n")
  
  # find out things about the table, then wrap the r function
  res <- dbSendQuery(conn,paste0("SELECT * FROM ",table," LIMIT 1"))
  dbnames <- res@env$info$names
  dbtypes <- res@env$info$dbtypes
  dbfun <- paste0("CREATE FUNCTION ",dbfunname,"(",paste0(dbnames," ", dbtypes, collapse=", "),
                  ") \nRETURNS TABLE(retval ",rettype,") LANGUAGE R {\n# rename arguments\n.dbdata <- data.frame(",
                  paste0(dbnames, collapse=", "),")\n",dbrcode,"};\n")
  # call the function we just created
  dbsel <- paste0("SELECT * FROM ", dbfunname, "( (SELECT * FROM ", table, " AS t) );\n")
  # ok, talk to DB (EZ)
  dbBegin(conn)
  dbSendQuery(conn,dbfun)
  res <- dbGetQuery(conn,dbsel)
  dbRollback(conn)
  return(res[,1])
})


