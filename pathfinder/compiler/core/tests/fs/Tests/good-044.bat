@echo off
@prompt # $t $g

@set NAME=%1

pf -s12 < %NAME%.xq
@echo %ERRORLEVEL%
