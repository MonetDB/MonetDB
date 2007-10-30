@echo off

call monetdb-clients-config --internal

set testpath=%CLIENTS_PREFIX%\lib\MonetDB\Tests

prompt # $t $g  
echo on

php -n -d "extension_dir=%phpextensiondir%" -f "%testpath%\sqlsample.php"
