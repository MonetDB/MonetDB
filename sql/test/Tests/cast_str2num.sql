start transaction;
create table T ( I int, F float ) ;

insert into T values ( 1, 0.1 );
insert into T values ( 2, 0.2 );
insert into T values ( 3, 0.3 );

select * from T where I <  2;
select * from T where I <  '2';
select * from T where F <  0.2;
select * from T where F <  '0.2';

select * from T where I <= 2;
select * from T where I <= '2';
select * from T where F <= 0.2;
select * from T where F <= '0.2';

select * from T where I  = 2;
select * from T where I  = '2';
select * from T where F  = 0.2;
select * from T where F  = '0.2';

select * from T where I >= 2;
select * from T where I >= '2';
select * from T where F >= 0.2;
select * from T where F >= '0.2';

select * from T where I >  2;
select * from T where I >  '2';
select * from T where F >  0.2;
select * from T where F >  '0.2';

drop table T;

commit;
