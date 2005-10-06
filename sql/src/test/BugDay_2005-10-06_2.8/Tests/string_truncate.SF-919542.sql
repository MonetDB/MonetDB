create table z(w varchar(0));
rollback;
create table z(w varchar(1));
insert into z values('wrong');
select * from z;
