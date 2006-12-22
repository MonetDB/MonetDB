@echo off

set NAME=%1

call Mlog.bat   "%MSERVER% --dbname=%TSTDB% %NAME%.mil"
call             %MSERVER% --dbname=%TSTDB% %NAME%.mil

call Mlog.bat   "%MSERVER% --dbname=%TSTDB% < %NAME%.mil"
call             %MSERVER% --dbname=%TSTDB% < %NAME%.mil

