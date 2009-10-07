@prompt # $t $g  
@echo on

echo user=invalid> .monetdb
echo password=invalid>> .monetdb
%SQL_CLIENT%
del .monetdb
