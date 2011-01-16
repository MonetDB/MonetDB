@echo off

set NAME=%1

prompt # $t $g  
echo on

call %MSERVER% --dbname=%TSTDB% %NAME%.mil

call %MSERVER% --dbname=%TSTDB% < %NAME%.mil

