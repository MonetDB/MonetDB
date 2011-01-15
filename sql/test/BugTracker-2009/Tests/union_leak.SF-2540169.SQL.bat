@prompt # $t $g
@echo on

%SQL_CLIENT% "%TSTTRGDIR%\%1%.1.sql"
%MAL_CLIENT% "%TSTTRGDIR%\%1%.2.mal"
%SQL_CLIENT% "%TSTTRGDIR%\%1%.3.sql"
%MAL_CLIENT% "%TSTTRGDIR%\%1%.2.mal"
%SQL_CLIENT% "%TSTTRGDIR%\%1%.3.sql"
%MAL_CLIENT% "%TSTTRGDIR%\%1%.2.mal"
%SQL_CLIENT% "%TSTTRGDIR%\%1%.4.sql"
