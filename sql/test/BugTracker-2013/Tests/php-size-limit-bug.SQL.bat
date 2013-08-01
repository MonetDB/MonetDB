@echo off

prompt # $t $g  
echo on

php -d "include_path=%PHP_INCPATH%" -f "%TSTSRCDIR%\php-size-limit-bug.php" %MAPIPORT% %TSTDB%
