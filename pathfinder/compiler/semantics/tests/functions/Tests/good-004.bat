@echo off
@prompt # $t $g

@set NAME=%1

pf -s6 < %NAME%.xq
@echo %ERRORLEVEL%
