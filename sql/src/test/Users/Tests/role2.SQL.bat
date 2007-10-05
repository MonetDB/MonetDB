@echo off

set SQL="mclient -lsql -umy_user2 -Pp2 -h %HOST% -p %MAPIPORT%"

call %SQL% < %RELSRCDIR%\..\role.sql
