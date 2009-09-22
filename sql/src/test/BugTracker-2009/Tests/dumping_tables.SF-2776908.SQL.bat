@echo on
@prompt # $t $g  

    %SQL_CLIENT% -i < %TSTSRCDIR%/../dumping_tables.SF-2776908.sql
