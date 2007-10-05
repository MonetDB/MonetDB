@echo off

set SQL="mclient -lsql -umonetdb -Pmonetdb -h %HOST% -p %MAPIPORT%"

echo Views Restrictions
call %SQL% < %RELSRCDIR%\..\views_restrictions.sql
echo step 1

echo Cleanup
echo step2
