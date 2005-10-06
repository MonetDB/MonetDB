@echo off
@prompt # $t $g

@set NAME=%1

pf -s4 < %NAME%.xq
@echo %ERRORLEVEL%
