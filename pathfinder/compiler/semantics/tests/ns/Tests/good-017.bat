@echo off
@prompt # $t $g

@set NAME=%1

pf -s7 < %NAME%.xq
@echo %ERRORLEVEL%

