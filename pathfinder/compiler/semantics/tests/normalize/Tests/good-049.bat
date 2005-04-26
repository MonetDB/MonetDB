@echo off
@prompt # $t $g

@set NAME=%1

pf -s2 < %NAME%.xq
@echo %ERRORLEVEL%
