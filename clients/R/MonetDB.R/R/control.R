monetdb.server.start <-
  function( bat.file ){
    
    if ( .Platform$OS.type == "unix" ) {
      if( !file.exists( bat.file ) ) stop( paste( bat.file , "does not exist. Run monetdb.server.setup() to create a batch file." ) )
      
      # uugly, find path of pid file again by parsing shell script.
      sc <- read.table(bat.file,sep="\n",stringsAsFactors=F)
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


monetdb.server.stop <-
  function( correct.pid ){
    
    if ( .Platform$OS.type == "unix" ) {
      system(paste0("kill ",correct.pid))
    } 
    
    if ( .Platform$OS.type == "windows" ) {
      
      # write the taskkill command line
      taskkill.cmd <- 
        paste( 
          "taskkill" , 
          "/PID" , 
          correct.pid ,
          "/F"
        )
      
      # kill the same process that was loaded
      system( taskkill.cmd )
      
    }
  }

monetdb.server.setup <-
  function( 
    
    # set the path to the directory where the initialization batch file and all data will be stored
    database.directory ,
    # must be empty or not exist
    
    # find the main path to the monetdb installation program
    monetdb.program.path ,
    
    # choose a database name
    dbname = "demo" ,
    
    # choose a database port
    # this port should not conflict with other monetdb databases
    # on your local computer.  two databases with the same port number
    # cannot be accessed at the same time
    dbport = 50000
  ){
    
    
    
    
    # switch all slashes to match windows
    monetdb.program.path <- normalizePath( monetdb.program.path , mustWork = FALSE )
    database.directory <- normalizePath( database.directory , mustWork = FALSE )
    
    
    # determine that the monetdb.program.path has been correctly specified #
    
    # first find the alleged path of mclient.exe
    mcl <- file.path( monetdb.program.path , "bin" )
    
    # then confirm it exists
    if( !file.exists( mcl ) ) stop( paste( mcl , "does not exist.  are you sure monetdb.program.path has been specified correctly?" ) )
    
    
    # confirm that the database directory is either empty or does not exist
    
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
      bfl <- paste0(bfl,".bat")
      
      
      # store all file lines for the .bat into a character vector
      bat.contents <-
        c(
          '@echo off' ,
          
          'setlocal' ,
          
          'rem figure out the folder name' ,
          paste0( 'set MONETDB=' , monetdb.program.path ) ,
          
          'rem extend the search path with our EXE and DLL folders' ,
          'rem we depend on pthreadVC2.dll having been copied to the lib folder' ,
          'set PATH=%MONETDB%\\bin;%MONETDB%\\lib;%MONETDB%\\lib\\MonetDB5;%PATH%' ,
          
          'rem prepare the arguments to mserver5 to tell it where to put the dbfarm' ,
          
          'if "%APPDATA%" == "" goto usevar' ,
          'rem if the APPDATA variable does exist, put the database there' ,
          paste0( 'set MONETDBDIR=' , database.directory , '\\' ) ,
          paste0( 'set MONETDBFARM="--dbpath=' , dbfl , '"' ) ,
          'goto skipusevar' ,
          ':usevar' ,
          'rem if the APPDATA variable does not exist, put the database in the' ,
          'rem installation folder (i.e. default location, so no command line argument)' ,
          'set MONETDBDIR=%MONETDB%\\var\\MonetDB5' ,
          'set MONETDBFARM=' ,
          ':skipusevar' ,
          
          'rem the SQL log directory used to be in %MONETDBDIR%, but we now' ,
          'rem prefer it inside the dbfarm, so move it there' ,
          
          'if not exist "%MONETDBDIR%\\sql_logs" goto skipmove' ,
          paste0( 
            'for /d %%i in ("%MONETDBDIR%"\\sql_logs\\*) do move "%%i" "%MONETDBDIR%\\' ,
            dbname , 
            '"\\%%~ni\\sql_logs'
          ) ,
          'rmdir "%MONETDBDIR%\\sql_logs"' ,
          ':skipmove' ,
          
          'rem start the real server' ,
          paste0( 
            '"%MONETDB%\\bin\\mserver5.exe" --set "prefix=%MONETDB%" --set "exec_prefix=%MONETDB%" %MONETDBFARM% %* --set mapi_port=' ,
            dbport 
          ) ,
          
          'if ERRORLEVEL 1 pause' ,
          
          'endlocal'
          
        )
      
    }
    
    if ( .Platform$OS.type == "unix" ) { # create shell script
      bfl <- paste0(bfl,".sh")
      bat.contents <- c('#!/bin/sh',
                        paste0( monetdb.program.path,
                                '/bin/mserver5 --set prefix=',monetdb.program.path,' --set exec_prefix=',monetdb.program.path,' --dbpath ',paste0(database.directory,"/",dbname),' --set mapi_port=' ,
                                dbport, " --daemon yes > /dev/null &" 
                        ),paste0("echo $! > ",database.directory,"/mserver5.started.from.R.pid"))
      
      
    }
    
    # write the .bat contents to a file on the local disk
    writeLines( bat.contents , bfl )
    if ( .Platform$OS.type == "unix" ) {
      Sys.chmod(bfl,mode="755")
    }
    # return the filepath to the batch file
    bfl
  }