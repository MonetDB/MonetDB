-- test robustness against small tables
create table xtmp2( i integer);
select * from storage where "table"= 'xtmp2';
call orderidx('sys','xtmp2','i');
select * from storage where "table"= 'xtmp2';
select * from xtmp2 where i>=0 and i <8;

create table xtmp3( i integer);
insert into xtmp3 values(3);
select * from storage where "table"= 'xtmp3';
call orderidx('sys','xtmp3','i');
select * from storage where "table"= 'xtmp3';
select * from xtmp3 where i>=0 and i <8;

destroy table xtmp2;
destroy table xtmp3;

