
create table xx( id serial, ival int);

insert into xx(ival) values(1),(2),(3),(4),(5),(6);

select * from xx;

create view iview as 
select id,ival from xx where ival >=2 and ival <5;

select * from iview;

select * from iview where ival between 2 and 5;
select * from iview where ival between 3 and 5;
select * from iview where ival between 4 and 5;
select * from iview where ival between 2 and 4;

drop view iview;
drop table xx;
