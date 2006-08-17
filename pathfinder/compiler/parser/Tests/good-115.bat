@echo off
@prompt # $t $g

@set NAME=%1

pf -s1 < %NAME%.xq
@echo %ERRORLEVEL%
