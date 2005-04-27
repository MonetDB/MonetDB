@echo off
@prompt # $t $g  

if not exist ID.1078462_doc.xml copy /y "%RELSRCDIR%\ID.1078462_doc.xml" ID.1078462_doc.xml > nul

pf-shred ID.1078462_doc.xml
