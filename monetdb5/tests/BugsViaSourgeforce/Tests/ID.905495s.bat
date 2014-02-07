@echo off

set NAME=%1

prompt # $t $g  
echo on

call %MSERVER% "--dbpath=%GDK_DBFARM%\%TSTDB%" %NAME%.mal

call %MSERVER% "--dbpath=%GDK_DBFARM%\%TSTDB%" < %NAME%.mal

