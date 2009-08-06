start transaction;
create table tmp(col int, col2 varchar(3));
insert into tmp values(0,'llllll');
rollback;
insert into tmp values(0,'ddd');

