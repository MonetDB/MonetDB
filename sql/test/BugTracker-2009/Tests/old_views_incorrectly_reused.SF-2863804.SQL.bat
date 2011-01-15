@prompt # $t $g
@echo on

%SQL_CLIENT% "%TSTTRGDIR%\%1%.def.sql"
%SQL_CLIENT% "%TSTTRGDIR%\%1%.view1.sql"
%SQL_CLIENT% "%TSTTRGDIR%\%1%.view2.sql"
