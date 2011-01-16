@echo off

set NAME=%1

prompt # $t $g  
echo on

call %MSERVER% --dbname=%TSTDB% %NAME%.milS

call %MSERVER% --dbname=%TSTDB% < %NAME%.milS

