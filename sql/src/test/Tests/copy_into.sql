create table copyouttest ( str VARCHAR(20), str2 VARCHAR(20));
insert into copyouttest values ('', 'test');
insert into copyouttest values ('test', '');
insert into copyouttest values ('','');
insert into copyouttest values (' Test ','');
select * from copyouttest;

copy select * from copyouttest into 'x.dat' delimiters '[]', '\n';
drop table copyouttest;

create table copyintest ( str VARCHAR(20), str2 VARCHAR(20));
copy into copyintest from 'x.dat' delimiters '[]', '\n';

select * from copyintest;
drop table copyintest;

