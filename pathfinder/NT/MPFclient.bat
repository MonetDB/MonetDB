@rem figure out the folder name
@set MONETDB=%~dp0

@rem remove the final backslash from the path
@set MONETDB=%MONETDB:~0,-1%

@rem start the real client
@"%MONETDB%\MapiClient.bat --language=xml" %*
