@echo off
@prompt # $t $g

@set NAME=%1

pf -s5 < %NAME%.xq
@echo %ERRORLEVEL%
