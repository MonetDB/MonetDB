@echo off

prompt # $t $g  
echo on

php -n -d "include_path=%PHP_INCPATH%" -f "%TSTSRCDIR%\php_int128.php" %MAPIPORT% %TSTDB%
