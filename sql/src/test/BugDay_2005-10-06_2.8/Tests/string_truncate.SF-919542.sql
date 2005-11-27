create table z(w varchar(0));
create table z(w varchar(1));
insert into z values('wrong');
select * from z;
drop table z;
