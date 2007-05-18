@echo off
@prompt # $t $g

@set NAME=%1

pf -s8 < %NAME%.xq
@echo %ERRORLEVEL%
