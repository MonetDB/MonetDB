start transaction;

create table rt (x real);
insert into rt values (2.1);
select x*x from rt;

rollback;
