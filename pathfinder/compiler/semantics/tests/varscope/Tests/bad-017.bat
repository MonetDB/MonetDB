@echo off
@prompt # $t $g

@set NAME=%1

rem cd to the test source directory, as that is where the modules are
cd %TSTSRCDIR%

pf -s7 < %NAME%.xq
@echo %ERRORLEVEL%
