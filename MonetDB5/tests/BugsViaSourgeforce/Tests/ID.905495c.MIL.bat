@echo off

set NAME=%1

call Mlog.bat   "%MIL_CLIENT% %NAME%.mil"
call             %MIL_CLIENT% %NAME%.mil

call Mlog.bat   "%MIL_CLIENT% < %NAME%.mil"
call             %MIL_CLIENT% < %NAME%.mil

