#!/bin/sh

Mlog -x "$SQL_CLIENT -s 'create table utf8test (s varchar(50))'"
Mlog -x "$SQL_CLIENT -s \"insert into utf8test values ('value without special characters')\""
Mlog -x "$SQL_CLIENT -s \"insert into utf8test values ('funny characters: àáâãäå')\""
Mlog -x "$SQL_CLIENT -fraw -s 'select * from utf8test'"
Mlog -x "$SQL_CLIENT -fsql -s 'select * from utf8test'"
Mlog -x "$SQL_CLIENT -fraw -Eiso-8859-1 -s 'select * from utf8test'"
Mlog -x "$SQL_CLIENT -fsql -Eiso-8859-1 -s 'select * from utf8test'"
Mlog -x "$SQL_CLIENT -s 'drop table utf8test'"
