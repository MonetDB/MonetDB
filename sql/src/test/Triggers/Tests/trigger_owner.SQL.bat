@echo off

set SQL="mclient -lsql -umonetdb -Pmonetdb -h %HOST% -p %MAPIPORT%"
set SQL1="mclient -lsql -uuser_test -Ppass -h %HOST% -p %MAPIPORT%"

echo trigger owner
call %SQL% < %RELSRCDIR%\..\trigger_owner_create.sql
call %SQL1% < %RELSRCDIR%\..\trigger_owner.sql
call %SQL% < %RELSRCDIR%\..\trigger_owner_drop.sql
echo done

