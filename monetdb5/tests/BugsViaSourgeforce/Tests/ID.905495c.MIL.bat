@echo off

set NAME=%1

prompt # $t $g  
echo on

call %MIL_CLIENT% %NAME%.mil

call %MIL_CLIENT% < %NAME%.mil

