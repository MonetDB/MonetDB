@echo off
@prompt # $t $g

@set NAME=%1

pf -s13 < %NAME%.xq
@echo %ERRORLEVEL%
