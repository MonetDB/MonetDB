@echo off

@rem Bug report #853867 says:
@rem "After running the script below for the 3rd time the server crashes in
@rem  vfprintf after an infinite recursive module load, TBL_loadmodule line
@rem  1582."
@rem Hence, we run the script 10 times, and see whether it works...

call Mlog.bat -x %MAPI_CLIENT% < %1.mil
call Mlog.bat -x %MAPI_CLIENT% < %1.mil
call Mlog.bat -x %MAPI_CLIENT% < %1.mil
call Mlog.bat -x %MAPI_CLIENT% < %1.mil
call Mlog.bat -x %MAPI_CLIENT% < %1.mil
call Mlog.bat -x %MAPI_CLIENT% < %1.mil
call Mlog.bat -x %MAPI_CLIENT% < %1.mil
call Mlog.bat -x %MAPI_CLIENT% < %1.mil
call Mlog.bat -x %MAPI_CLIENT% < %1.mil
call Mlog.bat -x %MAPI_CLIENT% < %1.mil

