CREATE TABLE ints ( val int );
prepare INSERT INTO ints VALUES ( ? - 20 );
exec **(1000);
select * from ints;
drop table ints;
