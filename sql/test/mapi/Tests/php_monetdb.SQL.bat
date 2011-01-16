@echo off

call monetdb-clients-config --internal

prompt # $t $g  
echo on

php -n -d "include_path=%datadir%\php" -f "%pkglibdir%\Tests\sqlsample.php" %MAPIPORT%
