@echo off
@prompt # $t $g

@set NAME=%1

pf -s3 < %NAME%.xq
@echo %ERRORLEVEL%
