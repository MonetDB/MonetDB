@echo on
@prompt # $t $g  

%SQL_CLIENT% -s "create table utf8test (s varchar(50))"
%SQL_CLIENT% -s "insert into utf8test values ('value without special characters')"
%SQL_CLIENT% -s "insert into utf8test values ('funny characters: àáâãäå')"
%SQL_CLIENT% -fraw -s "select * from utf8test"
%SQL_CLIENT% -fsql -s "select * from utf8test"
%SQL_CLIENT% -fraw -Eiso-8859-1 -s "select * from utf8test"
%SQL_CLIENT% -fsql -Eiso-8859-1 -s "select * from utf8test"
%SQL_CLIENT% -s "drop table utf8test"
