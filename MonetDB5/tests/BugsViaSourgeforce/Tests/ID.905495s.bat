@echo off

set NAME=%1

call Mlog.bat   "%MSERVER% --dbname=%TSTDB% %NAME%.mal"
call             %MSERVER% --dbname=%TSTDB% %NAME%.mal

call Mlog.bat   "%MSERVER% --dbname=%TSTDB% < %NAME%.mal"
call             %MSERVER% --dbname=%TSTDB% < %NAME%.mal

