START TRANSACTION;
create table T_1 ( c_1 int );
create table T_2 ( c_2 int );

insert into T_1 values ( 1 );
insert into T_1 values ( 2 );
insert into T_2 values ( 1 );
insert into T_2 values ( 2 );

select * from T_1, T_2 where c_1 = c_2;
select * from T_1, T_2 where c_1 = 1 and c_2 = 2;

drop table T_1;
drop table T_2;

commit;
