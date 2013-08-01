@echo off

prompt # $t $g  
echo on

php -d "include_path=%PHP_INCPATH%" -f "%TSTSRCBASE%\clients\examples\php\sqlsample.php" %MAPIPORT%
