create table ctmp( i integer, b boolean, f real);
call sys.compress('sys','ctmp');
call sys.decompress('sys','ctmp');
drop table ctmp;
