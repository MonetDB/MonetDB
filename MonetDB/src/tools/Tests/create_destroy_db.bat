@echo on
@prompt # $t $g  

mkdir %GDK_DBFARM%/TestDB1
echo quit; | %MSERVER% --dbname=TestDB1
@echo.
rmdir /s/q %GDK_DBFARM%/TestDB1

