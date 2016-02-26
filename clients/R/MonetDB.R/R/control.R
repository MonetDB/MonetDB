monetdb.server.start <-
  function( bat.file ){
    .Deprecated("Consider using MonetDBLite")
    if ( .Platform$OS.type == "unix" ) {
      if( !file.exists( bat.file ) ) stop( paste( bat.file , "does not exist. Run monetdb.server.setup() to create a batch file." ) )
      
      # uugly, find path of pid file again by parsing shell script.
      sc <- utils::read.table(bat.file,sep="\n",stringsAsFactors=F)
      pidfile <- substring(sc[[2,1]],11)
      
      # run script
      system(bat.file,wait=T)

      # read pid from file
      pid <- scan(pidfile,what=integer(),n=1)
      return(pid)
    } 
    if ( .Platform$OS.type == "windows" ) {
      
      # capture the result of a `tasklist` system call
      before.win.tasklist <- system2( 'tasklist' , stdout = TRUE )
      
      # store all pids before running the process
      before.pids <- substr( before.win.tasklist[ -(1:3) ] , 27 , 35 )
      
      # run the process
      shell.exec( bat.file )
      
      # start with an empty process id
      correct.pid <- character( 0 )
      
      # keep trying until a new `mserver` process appears
      while( length( correct.pid ) == 0 ){
        
        # capture the result of a `tasklist` system call
        after.win.tasklist <- system2( 'tasklist' , stdout = TRUE )
        
        # store all tasks after running the process
        after.tasks <- substr( after.win.tasklist[ -(1:3) ] , 1 , 25 )
        
        # store all pids after running the process
        after.pids <- substr( after.win.tasklist[ -(1:3) ] , 27 , 35 )
        
        # store the number in the task list containing the PIDs you've just initiated
        initiated.pid.positions <- which( !( after.pids %in% before.pids ) )
        
        # remove whitespace
        after.tasks <- gsub( " " , "" , after.tasks )
        
        # find the pid position that matches the executable file name
        correct.pid.position <- 
          intersect(
            grep( "mserver" , after.tasks ) ,
            initiated.pid.positions 
          )
        
        
        # remove whitespace
        correct.pid <- gsub( " " , "" , after.pids[ correct.pid.position ] )
        
      }
      
      # return the correct process ID
      return( correct.pid )
      # the process ID will then be used inside monet.kill() to end the mserver5.exe from within R
    }
    
  }

# oh the humanity
monetdb.server.stop <- function(correct.pid, wait=TRUE){
  .Deprecated("monetdb.server.shutdown")
  if (length(correct.pid) != 1) {
    stop("Need single pid, got ", correct.pid)
  }
  correct.pid <- as.integer(correct.pid)
  if (.Platform$OS.type == "windows")
    system(paste0("taskkill /F /PID ", correct.pid))
  else
    system(paste0("kill ", correct.pid))
  waittime <- 2
  if (!wait) return(TRUE)
  Sys.sleep(.5)
  repeat {
    stillrunning <- F
    if (.Platform$OS.type == "windows")
      stillrunning <- grepl("mserver5", system2('tasklist', c('/FI "PID eq ', correct.pid, '" /FO CSV'), stdout=T)[2])
    else
      stillrunning <- system(paste0("ps ax | grep \"^", correct.pid, ".*mserver5\""), ignore.stdout=T) == 0
    if (!stillrunning) break
    message("Waiting ", waittime, "s for server shutdown (ESC or CTRL+C to abort)")
    Sys.sleep(waittime)
    waittime <- waittime * 2
  }
}

monetdb.server.setup <-
  function( 
    
    # set the path to the directory where the initialization batch file and all data will be stored
    database.directory ,
    # must be empty or not exist
    
    # find the main path to the monetdb installation program
    monetdb.program.path ="",
    
    # choose a database name
    dbname = "demo" ,
    
    # choose a database port
    # this port should not conflict with other monetdb databases
    # on your local computer.  two databases with the same port number
    # cannot be accessed at the same time
    dbport = 50000
  ){
   .Deprecated("Consider using MonetDBLite")
    # switch all slashes to match windows
    monetdb.program.path <- normalizePath(getOption("monetdb.programpath.override", monetdb.program.path), mustWork = FALSE)
    # remove trailing slash from paths, otherwise the server won't start
    monetdb.program.path <- gsub("\\\\$|/$", "", monetdb.program.path)
    database.directory <- normalizePath( database.directory , mustWork = FALSE )
        
    # if the database directory does not exist, print that it's being created
    if ( !file.exists( database.directory ) ) {
      
      # create it
      dir.create( database.directory )
      # and say so
      message( paste( database.directory , "did not exist.  now it does" ) )
      
    } else {
      
      # otherwise confirm there's nothing in it
      if( length( list.files( database.directory , include.dirs = TRUE ) ) > 0 ) stop( 
        paste( 
          database.directory , 
          "must be empty.  either delete its contents or choose a different directory to store your database" 
        ) 
      )
      
    }
    
    # determine the batch file's location on the local disk
    bfl <- normalizePath( file.path( database.directory ,dbname  ) , mustWork = FALSE )
    
    # determine the dbfarm's location on the local disk
    dbfl <- normalizePath( file.path( database.directory , dbname ) , mustWork = FALSE )
    
    # create the dbfarm's directory
    dir.create( dbfl )
    if ( .Platform$OS.type == "windows" ) {
      bfl <- paste0(bfl, ".bat")

      # first find the alleged path of mclient.exe
      mcl <- file.path( monetdb.program.path , "bin" )
    
      # then confirm it exists
      if( !file.exists( mcl ) ) stop( paste( mcl , "does not exist.  are you sure monetdb.program.path has been specified correctly?" ) )
            
      # store all file lines for the .bat into a character vector
      bat.contents <-
        c(
          '@echo off' ,
          'setlocal' ,
          paste0( 'set MONETDB=' , monetdb.program.path ) ,
          'set PATH=%MONETDB%\\bin;%MONETDB%\\lib;%MONETDB%\\lib\\MonetDB5;%PATH%' ,
          paste0( 
            '"%MONETDB%\\bin\\mserver5.exe" --daemon=yes --set "prefix=%MONETDB%" --set "exec_prefix=%MONETDB%" "--dbpath=',dbfl,'" %* --set mapi_port=' , dbport) ,
          'if ERRORLEVEL 1 pause' ,
          'endlocal'
        ) 
    }
    
    if ( .Platform$OS.type == "unix" ) { # create shell script
      bfl <- paste0(bfl,".sh")
      bat.contents <- c('#!/bin/sh',
        paste0( ifelse(monetdb.program.path=="","",paste0(monetdb.program.path,"/")) ,
                'mserver5 --set prefix=',monetdb.program.path,' --set exec_prefix=',monetdb.program.path,' --dbpath ',paste0(database.directory,"/",dbname),' --set mapi_port=' ,
                dbport, " --daemon yes >> ",paste0(database.directory,"_",dbname,".log")," 2&>1 &" 
        ),paste0("echo $! > ",database.directory,"/mserver5.started.from.R.pid"))
    }
    
    # write the .bat contents to a file on the local disk
    writeLines( bat.contents , bfl )
    if (.Platform$OS.type == "unix" ) {
      Sys.chmod(bfl,mode="755")
    }
    # return the filepath to the batch file
    bfl
  }


monetdbd.liststatus <- monetdb.liststatus <- function(passphrase, host="localhost", port=50000L, timeout=86400L) {
  
  rawstr <- .monetdbd.command(passphrase, host, port, timeout)
  lines <- strsplit(rawstr, "\n", fixed=T)[[1]] # split by newline, first line is "=OK", so skip
  lines <- lines[grepl("^=sabdb:2:", lines)] # make sure we get a db list here, protocol v.2
  lines <- sub("=sabdb:2:", "", lines, fixed=T)
  # convert value into propert types etc
  dbdf <- as.data.frame(do.call("rbind", strsplit(lines, ",", fixed=T)), stringsAsFactors=F)

  names(dbdf) <- c("dbname", "uri", "locked", "state", "scenarios", "startCounter", "stopCounter", 
                   "crashCounter", "avgUptime", "maxUptime", "minUptime", "lastCrash", "lastStart", "lastStop", 
                   "crashAvg1", "crashAvg10", "crashAvg30")
  
  dbdf$locked <- dbdf$locked=="1"
  
  states <- c("illegal", "running", "crashed", "inactive", "starting")
  dbdf$state <- factor(states[as.integer(dbdf$state)+1])
  
  dbdf$startCounter <- as.numeric(dbdf$startCounter)
  dbdf$stopCounter <- as.numeric(dbdf$stopCounter)
  dbdf$crashCounter <- as.numeric(dbdf$crashCounter)
  
  dbdf$avgUptime <- as.numeric(dbdf$avgUptime)
  dbdf$maxUptime <- as.numeric(dbdf$maxUptime)
  dbdf$minUptime <- as.numeric(dbdf$minUptime)
  
  convertts <- function(col) {
    col[col=="-1"] <- NA
    return(as.POSIXct(as.numeric(col), origin="1970-01-01"))
  }
  dbdf$lastCrash <- convertts(dbdf$lastCrash)
  dbdf$lastStart <- convertts(dbdf$lastStart)
  dbdf$lastStop <- convertts(dbdf$lastStop)
  
  dbdf$crashAvg1 <- dbdf$crashAvg1=="1"
  dbdfcrashAvg10 <- as.numeric(dbdf$crashAvg10)
  dbdf$crashAvg30 <- as.numeric(dbdf$crashAvg30)
  dbdf$scenarios <- gsub("'", ", ", dbdf$scenarios, fixed=T)
  
  return(dbdf[order(dbdf$dbname), ])
}

monetdb.server.getpid <- function (con) {
  as.integer(dbGetQuery(con, "select value from env() where name='monet_pid'")[[1]])
}

# this is somewhat evil, no admin rights required to kill server. Even works on closed connections.
monetdb.server.shutdown <- function(con) {
  .Deprecated("Consider using MonetDBLite")
  stopifnot(inherits(con, "MonetDBConnection"))
  # reconnect with MAL scenario
  newparms <- con@connenv$params
  newparms$language <- "mal"
  newconn <- do.call("dbConnect", newparms)
  # construct and call MAL function that calls mal_exit()
  .mapiWrite(newconn@connenv$socket, "pattern exit():void address mal_exit; user.exit();\n")
  invisible(TRUE)
}
