@echo off

set path=%CLIENTS_PREFIX%\lib\MonetDB\Tests
set dir=%CLIENTS_PREFIX%\%PHP_EXTENSIONDIR%

call Mlog.bat -x @PHP@ -n -d extension_dir=%dir% -f %path%\sqlsample.php
