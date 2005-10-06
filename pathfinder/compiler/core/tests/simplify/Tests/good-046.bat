@echo off
@prompt # $t $g

@set NAME=%1

pf -s11 < %NAME%.xq
@echo %ERRORLEVEL%
