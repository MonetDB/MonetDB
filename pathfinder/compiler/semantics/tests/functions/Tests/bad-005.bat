@echo off
@prompt # $t $g

@set NAME=%1

pf -s9 < %NAME%.xq
@echo %ERRORLEVEL%
