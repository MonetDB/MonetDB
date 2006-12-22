@echo off

set NAME=%1

call Mlog.bat   "%MSERVER% --dbname=%TSTDB% %NAME%.milS"
call             %MSERVER% --dbname=%TSTDB% %NAME%.milS

call Mlog.bat   "%MSERVER% --dbname=%TSTDB% < %NAME%.milS"
call             %MSERVER% --dbname=%TSTDB% < %NAME%.milS

