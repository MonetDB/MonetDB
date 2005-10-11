@echo off
@prompt # $t $g

@set NAME=%1

@cd %TSTSRCDIR%

pf -s2 < %NAME%.xq
@echo %ERRORLEVEL%
