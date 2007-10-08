@echo off

set SQL="mclient -lsql -umy_user -Pp1 -h %HOST% -p %MAPIPORT%"

%SQL% < %RELSRCDIR%\..\table.sql
