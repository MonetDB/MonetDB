@rem figure out the folder name
@set MONETDB=%~dp0

@rem remove the final backslash from the path
@set MONETDB=%MONETDB:~0,-1%

@rem extend the search path with our EXE and DLL folders
@rem we depend on pthreadVCE.dll having been copied to the lib folder
@set PATH=%MONETDB%\bin;%MONETDB%\lib;%MONETDB%\lib\bin;%PATH%

@rem provide some helpful information about usage
@echo ^mclient interactive MonetDB/XQuery session: type an XQuery or XQUF update.
@echo ^Supported document-management XQuery extensions:
@echo ^ pf:collections() as node()
@echo ^ pf:documents($collectionName as xs:string) as node()
@echo ^ pf:del-doc($documentName as xs:string)
@echo ^ pf:add-doc($uri as xs:string, $documentName as xs:string
@echo ^     [,$collectionName as xs:string [,$freePercentage as xs:integer]])
@echo ^Session commands:
@echo ^?text   - send help message
@echo ^?       - show this message
@echo ^!       - shell escape
@echo ^<^<file  - read input from file
@echo ^>file   - save response in file
@echo ^>       - response to terminal
@echo ^cd      - change directory
@echo ^\\q     - terminate session
@echo ^\\T     - toggle timer
@echo ^<^>      - send query to server (or CTRL-Z)

@rem start the real client
@"%MONETDB%\bin\mclient.exe" --set "prefix=%MONETDB%" --set "exec_prefix=%MONETDB%" -lxquery %*
