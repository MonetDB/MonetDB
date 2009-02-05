@echo off
@prompt # $t $g

@set NAME=%1

pf -s10 < %NAME%.xq
@echo %ERRORLEVEL%
