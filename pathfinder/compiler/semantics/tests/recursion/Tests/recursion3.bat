@echo off
@prompt # $t $g

@set NAME=%1

pf -s14 < %NAME%.xq
@echo %ERRORLEVEL%
