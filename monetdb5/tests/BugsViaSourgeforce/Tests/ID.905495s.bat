@echo off

set NAME=%1

prompt # $t $g  
echo on

call %MSERVER% --dbname=%GDK_DBFARM%\%TSTDB% %NAME%.mal

call %MSERVER% --dbname=%GDK_DBFARM%\%TSTDB% < %NAME%.mal

