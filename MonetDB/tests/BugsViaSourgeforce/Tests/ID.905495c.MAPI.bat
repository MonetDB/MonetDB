@echo off

NAME=%1

call Mlog.bat   "%MAPI_CLIENT% %NAME%.mil"
cal         l    %MAPI_CLIENT% %NAME%.mil

call Mlog.bat   "%MAPI_CLIENT% < %NAME%.mil"
call             %MAPI_CLIENT% < %NAME%.mil

