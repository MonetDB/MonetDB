@echo off

set path=%CLIENTS_PREFIX%\lib\MonetDB\Tests
set dir=%CLIENTS_PREFIX%\%PHP_EXTENSIONDIR%

prompt # $t $g  
echo on

@PHP@ -n -d extension_dir=%dir% -f %path%\sqlsample.php
